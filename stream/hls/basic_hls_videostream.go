package hls

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/livepeer/lpms/segmenter"
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/m3u8"
)

const DefaultHLSStreamCap = uint(500)
const DefaultHLSStreamWin = uint(3)

// const DefaultMediaWinLen = uint(5)
const DefaultSegWaitTime = time.Second * 10
const SegWaitInterval = time.Second

var ErrAddHLSSegment = errors.New("ErrAddHLSSegment")

//BasicHLSVideoStream is a basic implementation of HLSVideoStream
type BasicHLSVideoStream struct {
	plCache    *m3u8.MediaPlaylist //StrmID -> MediaPlaylist
	segMap     map[string]segmenter.HLSSegment
	segNames   []string
	lock       sync.Locker
	strmID     string
	subscriber func(segmenter.HLSSegment, bool)
	winSize    uint
}

func NewBasicHLSVideoStream(strmID string, wSize uint) *BasicHLSVideoStream {
	pl, err := m3u8.NewMediaPlaylist(wSize, DefaultHLSStreamCap)
	if err != nil {
		return nil
	}

	return &BasicHLSVideoStream{
		plCache:  pl,
		segMap:   make(map[string]segmenter.HLSSegment),
		segNames: make([]string, 0),
		lock:     &sync.Mutex{},
		strmID:   strmID,
		winSize:  wSize,
	}
}

//SetSubscriber sets the callback function that will be called when a new hls segment is inserted
func (s *BasicHLSVideoStream) SetSubscriber(f func(seg segmenter.HLSSegment, eof bool)) {
	s.subscriber = f
}

//GetStreamID returns the streamID
func (s *BasicHLSVideoStream) GetStreamID() string { return s.strmID }

func (s *BasicHLSVideoStream) AppData() stream.AppData { return nil }

//GetStreamFormat always returns HLS
func (s *BasicHLSVideoStream) GetStreamFormat() stream.VideoFormat { return stream.HLS }

//GetStreamPlaylist returns the media playlist represented by the streamID
func (s *BasicHLSVideoStream) GetStreamPlaylist() (*m3u8.MediaPlaylist, error) {
	if s.plCache.Count() < s.winSize {
		return nil, nil
	}

	return s.plCache, nil
}

//GetHLSSegment gets the HLS segment.  It blocks until something is found, or timeout happens.
func (s *BasicHLSVideoStream) GetHLSSegment(segName string) (segmenter.HLSSegment, error) {
	seg, ok := s.segMap[segName]
	if !ok {
		return nil, ErrNotFound
	}
	return seg, nil
}

//AddHLSSegment adds the hls segment to the right stream
func (s *BasicHLSVideoStream) AddHLSSegment(seg segmenter.HLSSegment) error {
	if _, ok := s.segMap[seg.Name()]; ok {
		return nil //Already have the seg.
	}
	// glog.V(common.VERBOSE).Infof("Adding segment: %v", seg.Name)

	s.lock.Lock()
	defer s.lock.Unlock()

	//Add segment to media playlist and buffer
	segName := seg.Name()
	s.plCache.AppendSegment(seg.MediaSegment())
	s.segNames = append(s.segNames, segName)
	s.segMap[segName] = seg
	if s.plCache.Count() > s.winSize {
		s.plCache.Remove()
		toRemove := s.segNames[0]
		delete(s.segMap, toRemove)
		s.segNames = s.segNames[1:]
	}

	//Call subscriber
	if s.subscriber != nil {
		s.subscriber(seg, false)
	}

	return nil
}

func (s *BasicHLSVideoStream) End() {
	if s.subscriber != nil {
		s.subscriber(nil, true)
	}
}

func (s BasicHLSVideoStream) String() string {
	return fmt.Sprintf("StreamID: %v, Type: %v, len: %v", s.GetStreamID(), s.GetStreamFormat(), len(s.segMap))
}
