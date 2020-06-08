package segmenter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/golang/glog"
	iradix "github.com/hashicorp/go-immutable-radix"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/m3u8"
)

var ErrSegmenterTimeout = errors.New("SegmenterTimeout")
var ErrSegmenter = errors.New("SegmenterError")
var PlaylistRetryCount = 5
var PlaylistRetryWait = 500 * time.Millisecond

// SegmenterOptions specifies options for the segmenter
type SegmenterOptions struct {
	EnforceKeyframe bool //Enforce each segment starts with a keyframe
	SegLength       time.Duration
	StartSeq        int
}

// VideoSegment defines a segment of a video file or stream
type VideoSegment struct {
	Codec    av.CodecType
	Format   stream.VideoFormat
	Length   time.Duration
	Name     string
	SeqNo    uint64
	Filename string // filename for the video segment

	data     []byte
	lazydata func() ([]byte, error)
}

// Data provides lazy loading of data, segment file should be deleted
func (vs *VideoSegment) Data() ([]byte, error) {
	if vs.data == nil {
		seg, err := vs.lazydata()
		if err != nil {
			return nil, err
		}
		vs.data = seg
	}

	return vs.data, nil
}

// Cleanup removes the video segment file
func (vs *VideoSegment) Cleanup() {
	os.Remove(vs.Filename)
}

// VideoPlaylist defines a playlist for a specific video format
type VideoPlaylist struct {
	Format stream.VideoFormat
	// Data   []byte
	Data *m3u8.MediaPlaylist
}

// VideoSegmenter defines the interface for video segmenters
type VideoSegmenter interface {
	RTMPToHLS(ctx context.Context, cleanup bool) error
}

// FFMpegVideoSegmenter segments a RTMP stream by invoking FFMpeg and monitoring the file system.
type FFMpegVideoSegmenter struct {
	WorkDir        string
	LocalRtmpUrl   string
	StrmID         string
	curSegment     int
	priorSegment   int
	lock           *sync.Mutex
	curPlaylist    *m3u8.MediaPlaylist
	curPlWaitTime  time.Duration
	curSegWaitTime time.Duration
	SegLen         time.Duration

	watcher *dirWatcher

	// The hls video segment chan
	segChan chan *VideoSegment
}

// SegmentRecvChan is a receive channel for *VideoSegments
type SegmentRecvChan <-chan *VideoSegment

// CleanupFunc function
type CleanupFunc func()

// Set of directory watchers
var dirWatchers *dirWatcherMap = &dirWatcherMap{}

// NewFFMpegVideoSegmenter creates a new instance of the video segmenter
func NewFFMpegVideoSegmenter(workDir string, strmID string, localRtmpUrl string, opt SegmenterOptions) *FFMpegVideoSegmenter {
	if opt.SegLength == 0 {
		opt.SegLength = time.Second * 4
	}

	errHandler := func(err error) {
		// TODO let the dirWatcher keep track of its watches so that a new one
		// can be created if it dies but the segmenter is still running.  This
		// could cause created files to be missed.

		glog.V(4).Infof("dirWatcher error handler TODO")
	}

	dw, _ := dirWatchers.load(workDir)
	if dw == nil {
		dw, _ = newDirWatcher(workDir, errHandler)
	}

	dirWatchers.loadOrStore(workDir, dw)
	return &FFMpegVideoSegmenter{
		WorkDir:      workDir,
		StrmID:       strmID,
		LocalRtmpUrl: localRtmpUrl,
		SegLen:       opt.SegLength,
		curSegment:   opt.StartSeq,
		watcher:      dw,
		segChan:      make(chan *VideoSegment),
		lock:         &sync.Mutex{},
	}
}

// checkComplete checks if a video segment has been completed.  It knows this
// when the next segment file has been created.
func (s *FFMpegVideoSegmenter) checkComplete(event fsnotify.Event) (int, bool) {
	// Lock
	s.lock.Lock()
	defer s.lock.Unlock()

	// Does the event contain the next segemnt file
	name := fmt.Sprintf("%s/%s_%d.ts", s.WorkDir, s.StrmID, s.curSegment+1)
	var done bool
	var curSeg int
	if event.Name == name {
		done = true
		curSeg = s.curSegment
		s.curSegment++
	}

	return curSeg, done
}

// RTMPToHLSSegment segments an RTMP stream into HLS segments.  Segments are returned
// asynchronously via the SegmentRecvChan
func (s *FFMpegVideoSegmenter) RTMPToHLSSegment(ctx context.Context) (SegmentRecvChan, CleanupFunc, error) {
	// Set up local workdir
	if _, err := os.Stat(s.WorkDir); os.IsNotExist(err) {
		err := os.Mkdir(s.WorkDir, 0700)
		if err != nil {
			return nil, nil, err
		}
	}

	// Setup and call ffmpeg to segment the rtmp stream to hls, this is a
	// blocking operation so put in its own goroutine
	outp := fmt.Sprintf("%s/%s.m3u8", s.WorkDir, s.StrmID)
	tsTmpl := fmt.Sprintf("%s/%s", s.WorkDir, s.StrmID) + "_%d.ts"
	seglen := strconv.FormatFloat(s.SegLen.Seconds(), 'f', 6, 64)

	// Get the dirWatcher and register the streamID for event callbacks
	handler := func(event fsnotify.Event) {
		// Since we dont know when the segementer closes a file we have to wait
		// for the next create in order to determine that the prior file is closed
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("Segment handler panic: %v", r)
			}
		}()

		if event.Op&fsnotify.Create == fsnotify.Create {
			segment, ok := s.checkComplete(event)
			if ok {
				vs, err := s.newVideoSegment(segment)
				if err != nil {
					// log error
					glog.Errorf("Error creating new video segment, error: %v", err)
				}

				// TODO we should also return the error so should provide wrapper
				s.segChan <- vs
			}
		}
	}

	// Directory to watch
	hn := fmt.Sprintf("%s/%s", s.WorkDir, s.StrmID)
	s.watcher.AddHandler(hn, handler)
	go func() {
		// This will block until stream is closed
		// TODO test that when rtmp stream is closed the segmenter stream is also closed
		ret := ffmpeg.RTMPToHLS(s.LocalRtmpUrl, outp, tsTmpl, seglen, s.curSegment)
		if ret != nil {
			glog.V(4).Infof("RTMP to HLS segmenting completed with error: %v", ret)
		} else {
			glog.V(4).Infof("RTMP to HLS segmenting completed successfully")
		}

		// if the stream was terminated we could have an outstanding segment
		// that didn't get detected because we won't have anymore segment files
		// created to cause the event to trigger completion of prior segment file
		// Fabricate create event this to cause completion of current segment
		name := fmt.Sprintf("%s/%s_%d.ts", s.WorkDir, s.StrmID, s.curSegment+1)
		handler(fsnotify.Event{
			Name: name,
			Op:   fsnotify.Create,
		})

		// Remove stream template from dir watcher
		s.watcher.RemoveHandler(hn)

		// Close the VideoSegment channel since segmenting is done
		close(s.segChan)
		return
	}()

	// The cleanup function
	cfunc := func() {
		s.Cleanup()
	}

	return s.segChan, cfunc, nil
}

func readFileFully(name string) ([]byte, error) {
	if _, err := os.Stat(name); err != nil {
		return nil, err
	}

	content, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	return content, err
}

func (s *FFMpegVideoSegmenter) newVideoSegment(segment int) (*VideoSegment, error) {
	name := fmt.Sprintf("%s_%d.ts", s.StrmID, segment)
	plfn := fmt.Sprintf("%s/%s.m3u8", s.WorkDir, s.StrmID)
	var length time.Duration
	var err error

	// Use a playlist to determine the duration of the segment
	for i := 0; i < PlaylistRetryCount; i++ {
		pl, _ := m3u8.NewMediaPlaylist(uint(segment+1), uint(segment+1))
		content := readPlaylist(plfn)
		pl.DecodeFrom(bytes.NewReader(content), true)
		for _, plSeg := range pl.Segments {
			if plSeg != nil && plSeg.URI == name {
				length, err = time.ParseDuration(fmt.Sprintf("%vs", plSeg.Duration))
				break
			}
		}
		if length != 0 {
			break
		}
		if i < PlaylistRetryCount {
			glog.V(4).Infof("Waiting to load duration from playlist")
			time.Sleep(PlaylistRetryWait)
			continue
		} else {
			length, err = time.ParseDuration(fmt.Sprintf("%vs", pl.TargetDuration))
		}
	}

	// glog.Infof("Segment: %v, len:%v", name, len(seg))
	filename := fmt.Sprintf("%s/%s", s.WorkDir, name)
	return &VideoSegment{
		Codec:    av.H264,
		Format:   stream.HLS,
		Length:   length,
		Name:     name,
		SeqNo:    uint64(segment),
		Filename: filename,
		lazydata: func() ([]byte, error) {
			seg, err := readFileFully(filename)
			if err != nil {
				// log error
				glog.Errorf("Error reading HLS video segment file, error: %v", err)
				return nil, err
			}

			return seg, nil
		},
	}, err
}

//RTMPToHLS invokes FFMpeg to do the segmenting. This method blocks until the segmenter exits.
func (s *FFMpegVideoSegmenter) RTMPToHLS(ctx context.Context, cleanup bool) error {
	// Set up local workdir
	if _, err := os.Stat(s.WorkDir); os.IsNotExist(err) {
		err := os.Mkdir(s.WorkDir, 0700)
		if err != nil {
			return err
		}
	}

	outp := fmt.Sprintf("%s/%s.m3u8", s.WorkDir, s.StrmID)
	ts_tmpl := fmt.Sprintf("%s/%s", s.WorkDir, s.StrmID) + "_%d.ts"
	seglen := strconv.FormatFloat(s.SegLen.Seconds(), 'f', 6, 64)
	ret := ffmpeg.RTMPToHLS(s.LocalRtmpUrl, outp, ts_tmpl, seglen, s.curSegment)
	if cleanup {
		s.Cleanup()
	}
	return ret
}

//PollSegment monitors the filesystem and returns a new segment as it becomes available
func (s *FFMpegVideoSegmenter) PollSegment(ctx context.Context) (*VideoSegment, error) {
	var length time.Duration
	curTsfn := s.WorkDir + "/" + s.StrmID + "_" + strconv.Itoa(s.curSegment) + ".ts"
	nextTsfn := s.WorkDir + "/" + s.StrmID + "_" + strconv.Itoa(s.curSegment+1) + ".ts"
	seg, err := s.pollSegment(ctx, curTsfn, nextTsfn, time.Millisecond*100)
	if err != nil {
		return nil, err
	}

	name := s.StrmID + "_" + strconv.Itoa(s.curSegment) + ".ts"
	plfn := fmt.Sprintf("%s/%s.m3u8", s.WorkDir, s.StrmID)

	for i := 0; i < PlaylistRetryCount; i++ {
		pl, _ := m3u8.NewMediaPlaylist(uint(s.curSegment+1), uint(s.curSegment+1))
		content := readPlaylist(plfn)
		pl.DecodeFrom(bytes.NewReader(content), true)
		for _, plSeg := range pl.Segments {
			if plSeg != nil && plSeg.URI == name {
				length, err = time.ParseDuration(fmt.Sprintf("%vs", plSeg.Duration))
				break
			}
		}
		if length != 0 {
			break
		}
		if i < PlaylistRetryCount {
			glog.V(4).Infof("Waiting to load duration from playlist")
			time.Sleep(PlaylistRetryWait)
			continue
		} else {
			length, err = time.ParseDuration(fmt.Sprintf("%vs", pl.TargetDuration))
		}
	}

	s.curSegment = s.curSegment + 1
	// glog.Infof("Segment: %v, len:%v", name, len(seg))
	return &VideoSegment{Codec: av.H264, Format: stream.HLS, Length: length, data: seg, Name: name, SeqNo: uint64(s.curSegment - 1)}, err
}

//PollPlaylist monitors the filesystem and returns a new playlist as it becomes available
func (s *FFMpegVideoSegmenter) PollPlaylist(ctx context.Context) (*VideoPlaylist, error) {
	plfn := fmt.Sprintf("%s/%s.m3u8", s.WorkDir, s.StrmID)
	var lastPl []byte
	if s.curPlaylist == nil {
		lastPl = nil
	} else {
		lastPl = s.curPlaylist.Encode().Bytes()
	}

	pl, err := s.pollPlaylist(ctx, plfn, time.Millisecond*100, lastPl)
	if err != nil {
		return nil, err
	}

	p, err := m3u8.NewMediaPlaylist(50000, 50000)
	err = p.DecodeFrom(bytes.NewReader(pl), true)
	if err != nil {
		return nil, err
	}

	s.curPlaylist = p
	return &VideoPlaylist{Format: stream.HLS, Data: p}, err
}

func readPlaylist(fn string) []byte {
	if _, err := os.Stat(fn); err == nil {
		content, err := ioutil.ReadFile(fn)
		if err != nil {
			return nil
		}
		return content
	}

	return nil
}

func (s *FFMpegVideoSegmenter) pollPlaylist(ctx context.Context, fn string, sleepTime time.Duration, lastFile []byte) (f []byte, err error) {
	for {
		if _, err := os.Stat(fn); err == nil {
			if err != nil {
				return nil, err
			}

			content, err := ioutil.ReadFile(fn)
			if err != nil {
				return nil, err
			}

			//The m3u8 package has some bugs, so the translation isn't 100% correct...
			p, err := m3u8.NewMediaPlaylist(50000, 50000)
			err = p.DecodeFrom(bytes.NewReader(content), true)
			if err != nil {
				return nil, err
			}
			curFile := p.Encode().Bytes()

			// fmt.Printf("p.Segments: %v\n", p.Segments[0])
			// fmt.Printf("lf: %s \ncf: %s \ncomp:%v\n\n", lastFile, curFile, bytes.Compare(lastFile, curFile))
			if lastFile == nil || bytes.Compare(lastFile, curFile) != 0 {
				s.curPlWaitTime = 0
				return content, nil
			}
		}

		select {
		case <-ctx.Done():
			glog.V(4).Infof("ctx.Done()!!!")
			return nil, ctx.Err()
		default:
		}

		if s.curPlWaitTime >= 10*s.SegLen {
			return nil, ErrSegmenterTimeout
		}
		time.Sleep(sleepTime)
		s.curPlWaitTime = s.curPlWaitTime + sleepTime
	}

}

func (s *FFMpegVideoSegmenter) pollSegment(ctx context.Context, curFn string, nextFn string, sleepTime time.Duration) (f []byte, err error) {
	var content []byte
	for {
		//Because FFMpeg keeps appending to the current segment until it's full before moving onto the next segment, we monitor the existance of
		//the next file as a signal for the completion of the current segment.
		if _, err := os.Stat(nextFn); err == nil {
			content, err = ioutil.ReadFile(curFn)
			if err != nil {
				return nil, err
			}
			s.curSegWaitTime = 0
			return content, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if s.curSegWaitTime > 10*s.SegLen {
			return nil, ErrSegmenterTimeout
		}

		time.Sleep(sleepTime)
		s.curSegWaitTime = s.curSegWaitTime + sleepTime
	}
}

func (s *FFMpegVideoSegmenter) Cleanup() {
	glog.V(4).Infof("Cleaning up video segments.....")
	files, _ := filepath.Glob(path.Join(s.WorkDir, s.StrmID) + "*")
	for _, fn := range files {
		os.Remove(fn)
	}
}

// dirWatcher watches a directory for events and notifies a dispatcher
// with the events
type dirWatcher struct {
	workDir      string
	watcher      *fsnotify.Watcher
	errorHandler ErrorHandlerFunc

	// Callback handlers for stream id's
	lock      *sync.RWMutex
	radixTree *iradix.Tree
}

// DispatchFunc provides callback function for events generated by the watcher
type DispatchFunc func(fsnotify.Event)

// ErrorHandlerFunc provides for error callback when the watcher encounters and
// error
type ErrorHandlerFunc func(error)

// ShutdownError indicates the watcher has shutdown
type ShutdownError struct{}

func (ShutdownError) Error() string {
	return "directory watcher shutdown due to event channel closed"
}

// default handlers
var defaultDispatcher = func(fsnotify.Event) {}
var defaultErrorHandler = func(error) {}

func newDirWatcher(dir string, errHandler ErrorHandlerFunc) (*dirWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		glog.V(4).Infof("Error creating fsnotify.Watcher: %v", err)
		return nil, err
	}

	// Always watch the directory
	if err = watcher.Add(dir); err != nil {
		glog.V(4).Infof("Error watching diretory: %v", err)
		return nil, err
	}

	if errHandler == nil {
		errHandler = defaultErrorHandler
	}

	dw := &dirWatcher{workDir: dir,
		watcher:      watcher,
		errorHandler: errHandler,
		lock:         &sync.RWMutex{},
		radixTree:    iradix.New()}

	// Start go routine to watch the directory
	go func() {
		lastEv := fsnotify.Event{}
		timeout := time.Hour * 24

		for {
			select {
			case event, ok := <-dw.watcher.Events:
				if !ok {
					// dispatch a shutdown
					dw.errorHandler(&ShutdownError{})
					return
				}

				// Dispatch event for processing
				if lastEv.Name != event.Name || lastEv.Op != event.Op {
					dw.Dispatch(event)

					// Capture the last event and timeout after 1 second to reset
					lastEv = event
					timeout = time.Second * 1
				}

			case err, ok := <-dw.watcher.Errors:
				// What kinds of errors and what to do
				if !ok {
					// Dispatch shutdonw, channel closed and empty
					dw.errorHandler(&ShutdownError{})
					return
				}

				// Log the error and continue
				glog.V(4).Infof("Error watching directory: %v", err)
				dw.errorHandler(err)

			case <-time.Tick(timeout):
				lastEv = fsnotify.Event{}
				timeout = time.Hour * 24
			}
		}
	}()

	return dw, nil
}

// AddHandler adds handler for files not yet created in workdir
func (dw *dirWatcher) AddHandler(name string, handler DispatchFunc) {
	dw.lock.Lock()
	defer dw.lock.Unlock()
	dw.radixTree, _, _ = dw.radixTree.Insert([]byte(name), handler)
}

func (dw *dirWatcher) RemoveHandler(name string) {
	dw.lock.Lock()
	defer dw.lock.Unlock()
	dw.radixTree, _, _ = dw.radixTree.Delete([]byte(name))
}

// Add starts watching for events on the specified file or directory
// in the watchers directory. Name must exist in the work dir
func (dw *dirWatcher) Add(name string, handler DispatchFunc) error {
	fn := fmt.Sprintf("%s/%s", dw.workDir, name)
	err := dw.watcher.Add(fn)
	if err == nil {
		dw.AddHandler(name, handler)
	}

	return err
}

// Remove stops watching the specified directory or file in the watchers directory.
func (dw *dirWatcher) Remove(name string) error {
	fn := fmt.Sprintf("%s/%s", dw.workDir, name)
	dw.RemoveHandler(name)
	return dw.watcher.Remove(fn)
}

func (dw *dirWatcher) Dispatch(event fsnotify.Event) {
	dw.lock.RLock()
	defer dw.lock.RUnlock()

	// Use the longest prefix match
	_, h, ok := dw.radixTree.Root().LongestPrefix([]byte(event.Name))
	if ok {
		// Recover handler
		handler, ok := h.(DispatchFunc)
		if ok {
			// Handle in goroutine
			go func() {
				handler(event)
			}()
		}
	}
}

func dumpKeys(dw *dirWatcher) {
	it := dw.radixTree.Root().Iterator()
	for key, _, ok := it.Next(); ok; key, _, ok = it.Next() {
		fmt.Printf("RadixTree key=%s\n", string(key))
	}
}

// Close closes the directory watcher.
func (dw *dirWatcher) Close() {
	dw.watcher.Close()
}

// dirWatcherMap provides a concurrent map of dirWatcher indexed by work dir
type dirWatcherMap sync.Map //[dir]*dirWatcher
func (m *dirWatcherMap) delete(dir string) {
	(*sync.Map)(m).Delete(dir)
}

func (m *dirWatcherMap) load(dir string) (value *dirWatcher, ok bool) {
	v, ok := (*sync.Map)(m).Load(dir)
	if v != nil {
		value = v.(*dirWatcher)
		return
	}

	return
}

func (m *dirWatcherMap) loadOrStore(dir string, value *dirWatcher) (actual *dirWatcher, loaded bool) {
	v, loaded := (*sync.Map)(m).LoadOrStore(dir, value)
	if v != nil {
		actual = v.(*dirWatcher)
	}

	return
}

func (m *dirWatcherMap) store(dir string, value *dirWatcher) {
	(*sync.Map)(m).Store(dir, value)
}
