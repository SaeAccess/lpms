package hls

import (
	"errors"

	"github.com/livepeer/lpms/segmenter"
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/m3u8"
)

var ErrNotFound = errors.New("Not Found")
var ErrBadHLSBuffer = errors.New("BadHLSBuffer")
var ErrEOF = errors.New("ErrEOF")

// HLSVideoManifest defines manifest for HLS
type HLSVideoManifest interface {
	stream.VideoManifest
	GetManifest() (*m3u8.MasterPlaylist, error)
	GetVideoStream(strmID string) (HLSVideoStream, error)
	// AddVideoStream(strmID string, variant *m3u8.Variant) (HLSVideoStream, error)
	AddVideoStream(strm HLSVideoStream, variant *m3u8.Variant) error
	GetStreamVariant(strmID string) (*m3u8.Variant, error)
	GetVideoStreams() []HLSVideoStream
	DeleteVideoStream(strmID string) error
}

// HLSVideoStream contains the master playlist, media playlists in it,
// and the segments in them.  Each media playlist also has a streamID.
// You can only add media playlists to the stream.
type HLSVideoStream interface {
	stream.VideoStream
	GetStreamPlaylist() (*m3u8.MediaPlaylist, error)
	// GetStreamVariant() *m3u8.Variant
	GetHLSSegment(segName string) (segmenter.HLSSegment, error)
	AddHLSSegment(seg segmenter.HLSSegment) error
	SetSubscriber(f func(seg segmenter.HLSSegment, eof bool))
	End()
}
