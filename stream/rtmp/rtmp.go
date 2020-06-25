package rtmp

import (
	"context"

	"github.com/livepeer/joy4/av"
	"github.com/livepeer/lpms/stream"
)

type RTMPVideoStream interface {
	stream.VideoStream
	ReadRTMPFromStream(ctx context.Context, dst av.MuxCloser) (eof chan struct{}, err error)
	WriteRTMPToStream(ctx context.Context, src av.DemuxCloser) (eof chan struct{}, err error)
	Close()
	Height() int
	Width() int
}
