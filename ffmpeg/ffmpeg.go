package ffmpeg

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"
)

// #cgo pkg-config: libavformat libavfilter libavcodec libavutil libswscale gnutls
// #include <stdlib.h>
// #include "lpms_ffmpeg.h"
import "C"

var ErrTranscoderRes = errors.New("TranscoderInvalidResolution")
var ErrTranscoderHw = errors.New("TranscoderInvalidHardware")
var ErrTranscoderInp = errors.New("TranscoderInvalidInput")

type Acceleration int

const (
	Software Acceleration = iota
	Nvidia
	Amd
)

type ComponentOptions struct {
	Name string
	Opts map[string]string
}

type TranscoderHandle *C.struct_transcode_thread

type TranscodeOptionsIn struct {
	Fname  string
	Accel  Acceleration
	Device string
	Handle TranscoderHandle
}

type TranscodeOptions struct {
	Oname   string
	Profile VideoProfile
	Accel   Acceleration
	Device  string

	Muxer        ComponentOptions
	VideoEncoder ComponentOptions
	AudioEncoder ComponentOptions
}

type MediaInfo struct {
	Frames int
	Pixels int64
}

type TranscodeResults struct {
	Handle  TranscoderHandle
	Decoded MediaInfo
	Encoded []MediaInfo
}

func RTMPToHLS(localRTMPUrl string, outM3U8 string, tmpl string, seglen_secs string, seg_start int) error {
	inp := C.CString(localRTMPUrl)
	outp := C.CString(outM3U8)
	ts_tmpl := C.CString(tmpl)
	seglen := C.CString(seglen_secs)
	segstart := C.CString(fmt.Sprintf("%v", seg_start))
	ret := int(C.lpms_rtmp2hls(inp, outp, ts_tmpl, seglen, segstart))
	C.free(unsafe.Pointer(inp))
	C.free(unsafe.Pointer(outp))
	C.free(unsafe.Pointer(ts_tmpl))
	C.free(unsafe.Pointer(seglen))
	C.free(unsafe.Pointer(segstart))
	if 0 != ret {
		glog.Infof("RTMP2HLS Transmux Return : %v\n", Strerror(ret))
		return ErrorMap[ret]
	}
	return nil
}

func Transcode(input string, workDir string, ps []VideoProfile) error {

	opts := make([]TranscodeOptions, len(ps))
	for i, param := range ps {
		oname := path.Join(workDir, fmt.Sprintf("out%v%v", i, filepath.Base(input)))
		opt := TranscodeOptions{
			Oname:   oname,
			Profile: param,
			Accel:   Software,
		}
		opts[i] = opt
	}
	inopts := &TranscodeOptionsIn{
		Fname: input,
		Accel: Software,
	}
	return Transcode2(inopts, opts)
}

func newAVOpts(opts map[string]string) *C.AVDictionary {
	var dict *C.AVDictionary
	for key, value := range opts {
		k := C.CString(key)
		v := C.CString(value)
		defer C.free(unsafe.Pointer(k))
		defer C.free(unsafe.Pointer(v))
		C.av_dict_set(&dict, k, v, 0)
	}
	return dict
}

// return encoding specific options for the given accel
func configAccel(inAcc, outAcc Acceleration, inDev, outDev string) (string, string, error) {
	switch inAcc {
	case Software:
		switch outAcc {
		case Software:
			return "libx264", "scale", nil
		case Nvidia:
			upload := "hwupload_cuda"
			if outDev != "" {
				upload = upload + "=device=" + outDev
			}
			return "h264_nvenc", upload + ",scale_cuda", nil
		}
	case Nvidia:
		switch outAcc {
		case Software:
			return "libx264", "scale_cuda", nil
		case Nvidia:
			// If we encode on a different device from decode then need to transfer
			if outDev != "" && outDev != inDev {
				return "", "", ErrTranscoderInp // XXX not allowed
			}
			return "h264_nvenc", "scale_cuda", nil
		}
	}
	return "", "", ErrTranscoderHw
}
func accelDeviceType(accel Acceleration) (C.enum_AVHWDeviceType, error) {
	switch accel {
	case Software:
		return C.AV_HWDEVICE_TYPE_NONE, nil
	case Nvidia:
		return C.AV_HWDEVICE_TYPE_CUDA, nil

	}
	return C.AV_HWDEVICE_TYPE_NONE, ErrTranscoderHw
}

func Transcode2(input *TranscodeOptionsIn, ps []TranscodeOptions) error {
	res, err := Transcode3(input, ps)
	if err != nil {
		return err
	}
	TranscodeStop(&res.Handle)
	return err
}

func Transcode3(input *TranscodeOptionsIn, ps []TranscodeOptions) (*TranscodeResults, error) {
	if input == nil {
		return nil, ErrTranscoderInp
	}
	hw_type, err := accelDeviceType(input.Accel)
	if err != nil {
		return nil, err
	}
	fname := C.CString(input.Fname)
	defer C.free(unsafe.Pointer(fname))
	params := make([]C.output_params, len(ps))
	for i, p := range ps {
		oname := C.CString(p.Oname)
		defer C.free(unsafe.Pointer(oname))

		param := p.Profile
		w, h, err := VideoProfileResolution(param)
		if err != nil {
			if "drop" != p.VideoEncoder.Name && "copy" != p.VideoEncoder.Name {
				return nil, err
			}
		}
		br := strings.Replace(param.Bitrate, "k", "000", 1)
		bitrate, err := strconv.Atoi(br)
		if err != nil {
			if "drop" != p.VideoEncoder.Name && "copy" != p.VideoEncoder.Name {
				return nil, err
			}
		}
		encoder, scale_filter := p.VideoEncoder.Name, "scale"
		if encoder == "" {
			encoder, scale_filter, err = configAccel(input.Accel, p.Accel, input.Device, p.Device)
			if err != nil {
				return nil, err
			}
		}
		// preserve aspect ratio along the larger dimension when rescaling
		filters := fmt.Sprintf("fps=%d/%d,%s='w=if(gte(iw,ih),%d,-2):h=if(lt(iw,ih),%d,-2)'", param.Framerate, 1, scale_filter, w, h)
		if input.Accel != Software && p.Accel == Software {
			// needed for hw dec -> hw rescale -> sw enc
			filters = filters + ":format=yuv420p,hwdownload"
		}
		muxOpts := C.component_opts{
			opts: newAVOpts(p.Muxer.Opts), // don't free this bc of avformat_write_header API
		}
		if p.Muxer.Name != "" {
			muxOpts.name = C.CString(p.Muxer.Name)
			defer C.free(unsafe.Pointer(muxOpts.name))
		}
		vidOpts := C.component_opts{
			name: C.CString(encoder),
			opts: newAVOpts(p.VideoEncoder.Opts),
		}
		audioEncoder := p.AudioEncoder.Name
		if audioEncoder == "" {
			audioEncoder = "aac"
		}
		audioOpts := C.component_opts{
			name: C.CString(audioEncoder),
			opts: newAVOpts(p.AudioEncoder.Opts),
		}
		vfilt := C.CString(filters)
		defer C.free(unsafe.Pointer(vidOpts.name))
		defer C.free(unsafe.Pointer(audioOpts.name))
		defer C.free(unsafe.Pointer(vfilt))
		fps := C.AVRational{num: C.int(param.Framerate), den: 1}
		params[i] = C.output_params{fname: oname, fps: fps,
			w: C.int(w), h: C.int(h), bitrate: C.int(bitrate),
			muxer: muxOpts, audio: audioOpts, video: vidOpts, vfilters: vfilt}
	}
	var device *C.char
	if input.Device != "" {
		device = C.CString(input.Device)
		defer C.free(unsafe.Pointer(device))
	}
	inp := &C.input_params{fname: fname, hw_type: hw_type, device: device,
		handle: input.Handle}
	results := make([]C.output_results, len(ps))
	decoded := &C.output_results{}
	var (
		paramsPointer  *C.output_params
		resultsPointer *C.output_results
	)
	if len(params) > 0 {
		paramsPointer = (*C.output_params)(&params[0])
		resultsPointer = (*C.output_results)(&results[0])
	}
	ret := int(C.lpms_transcode(inp, paramsPointer, resultsPointer, C.int(len(params)), decoded))
	if 0 != ret {
		glog.Error("Transcoder Return : ", Strerror(ret))
		return nil, ErrorMap[ret]
	}
	tr := make([]MediaInfo, len(ps))
	for i, r := range results {
		tr[i] = MediaInfo{
			Frames: int(r.frames),
			Pixels: int64(r.pixels),
		}
	}
	dec := MediaInfo{
		Frames: int(decoded.frames),
		Pixels: int64(decoded.pixels),
	}
	return &TranscodeResults{Handle: inp.handle, Encoded: tr, Decoded: dec}, nil
}

func TranscodeStop(handle *TranscoderHandle) {
	C.lpms_transcode_stop(*handle)
	handle = nil // do this here bc double pointer params are hard in cgo
}

func InitFFmpeg() {
	C.lpms_init()
}
