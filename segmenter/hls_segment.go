package segmenter

import (
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/m3u8"
)

// hlsVideoSegment wraps a Segmenter VideoSegment to provide the HLSSegment interface
// without copying the segment and provide lifecycle around the sement data file
type hlsVideoSegment struct {
	segment *VideoSegment
}

// NewHLSSegment transforms a video segment into a HLSSegment
func NewHLSSegment(seg *VideoSegment) stream.HLSSegment {
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

func (s hlsVideoSegment) WithNewFileName(filename string) stream.HLSSegment {
	// Copy this segment then set new filename
	vs := *s.segment
	vs.Filename = filename
	vs.data = nil
	return NewHLSSegment(&vs)
}
