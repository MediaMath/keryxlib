package pg

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "unsafe"

// LInt8 reads a little-endian int8 from the head of a slice
func LInt8(bs []byte) (i int8) {
	if len(bs) > 0 {
		u := uint8(LUint(bs[:1]))
		i = *(*int8)(unsafe.Pointer(&u))
	}
	return
}

// LInt16 reads a little-endian int16 from the head of a slice
func LInt16(bs []byte) (i int16) {
	if len(bs) > 1 {
		u := uint16(LUint(bs[:2]))
		i = *(*int16)(unsafe.Pointer(&u))
	}
	return
}

// LInt32 reads a little-endian int32 from the head of a slice
func LInt32(bs []byte) (i int32) {
	if len(bs) > 3 {
		u := uint32(LUint(bs[:4]))
		i = *(*int32)(unsafe.Pointer(&u))
	}
	return
}

// LInt64 reads a little-endian int64 from the head of a slice
func LInt64(bs []byte) (i int64) {
	if len(bs) > 7 {
		u := uint64(LUint(bs[:8]))
		i = *(*int64)(unsafe.Pointer(&u))
	}
	return
}

// LUint reads a little-endian uint64 from the head of a slice
func LUint(bs []byte) uint64 {
	return luintSwitch(bs)
}

// LSingle reads a little-endian float32 from the head of a slice
func LSingle(bs []byte) (f float32) {
	if len(bs) >= 4 {
		u := uint32(LUint(bs[:4]))
		f = *(*float32)(unsafe.Pointer(&u))
	}

	return
}

// LDouble reads a little-endian float64 from the head of a slice
func LDouble(bs []byte) (f float64) {
	if len(bs) >= 8 {
		u := LUint(bs[:8])
		f = *(*float64)(unsafe.Pointer(&u))
	}

	return
}

func luintSwitch(bs []byte) uint64 {
	var out uint64

	switch len(bs) {
	case 8:
		out |= uint64(bs[7]) << 56
		fallthrough
	case 7:
		out |= uint64(bs[6]) << 48
		fallthrough
	case 6:
		out |= uint64(bs[5]) << 40
		fallthrough
	case 5:
		out |= uint64(bs[4]) << 32
		fallthrough
	case 4:
		out |= uint64(bs[3]) << 24
		fallthrough
	case 3:
		out |= uint64(bs[2]) << 16
		fallthrough
	case 2:
		out |= uint64(bs[1]) << 8
		fallthrough
	case 1:
		out |= uint64(bs[0])
	}

	return out
}

func luintLoop(bs []byte) uint64 {
	var out uint64

	bsLen := len(bs)
	sft := uint(0)
	for i := 0; i < bsLen; i++ {
		out |= uint64(bs[i]) << sft
		sft += 8
	}

	return out
}
