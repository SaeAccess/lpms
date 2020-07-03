package segmenter

import (
	"context"

	"github.com/livepeer/m3u8"
)

type HLSDemuxer interface {
	PollPlaylist(ctx context.Context) (m3u8.MediaPlaylist, error)
	WaitAndPopSegment(ctx context.Context, name string) ([]byte, error)
	WaitAndGetSegment(ctx context.Context, name string) ([]byte, error)
}

type HLSMuxer interface {
	WriteSegment(seqNo uint64, name string, duration float64, s []byte) error
}

// HLSSegment defines the interface for HLS segments
// We couldn't just use the m3u8 definition
// Make this an interface, gives better control over copying of VideoSegment
// data and also reading of video segment files.
type HLSSegment interface {
	SeqNo() uint64
	Name() string
	Data() ([]byte, error)
	Duration() float64
	SegmentFileName() string

	// Transform segment in a playlist segment
	MediaSegment() *m3u8.MediaSegment
	Cleanup()

	// Make new instance of HLSSegment using data references
	WithData(filename string, data func() ([]byte, error)) HLSSegment
}

// hlsVideoSegment wraps a Segmenter VideoSegment to provide the HLSSegment interface
// without copying the segment and provide lifecycle around the sement data file
type hlsVideoSegment struct {
	segment *VideoSegment
}

// NewHLSSegment transforms a video segment into a HLSSegment
func NewHLSSegment(seg *VideoSegment) HLSSegment {
	return &hlsVideoSegment{segment: seg}
}

func (s hlsVideoSegment) SeqNo() uint64         { return s.segment.SeqNo }
func (s hlsVideoSegment) Name() string          { return s.segment.Name }
func (s hlsVideoSegment) Data() ([]byte, error) { return s.segment.Data() }

func (s hlsVideoSegment) Duration() float64       { return s.segment.Length.Seconds() }
func (s hlsVideoSegment) SegmentFileName() string { return s.segment.Filename }
func (s hlsVideoSegment) MediaSegment() *m3u8.MediaSegment {
	return &m3u8.MediaSegment{
		SeqId:    s.SeqNo(),
		Duration: s.Duration(),
		URI:      s.Name(),
	}
}

func (s hlsVideoSegment) Cleanup() { s.segment.Cleanup() }

func (s hlsVideoSegment) WithData(filename string, data func() ([]byte, error)) HLSSegment {
	// Copy this segment then set new filename
	vs := *s.segment
	vs.Filename = filename
	vs.data = nil
	vs.Lazydata = data
	return NewHLSSegment(&vs)
}
