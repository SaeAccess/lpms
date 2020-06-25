package core

import (
	"context"

	"github.com/livepeer/lpms/segmenter"
	"github.com/livepeer/lpms/stream/hls"
	"github.com/livepeer/lpms/stream/rtmp"
)

// RTMPSegmenter describes an interface for a segmenter
type RTMPSegmenter interface {
	SegmentRTMPToHLS(ctx context.Context, rs rtmp.RTMPVideoStream, hs hls.HLSVideoStream, segOptions segmenter.SegmenterOptions) error
}
