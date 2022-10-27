package utils

import (
	"net/http"
	"strconv"
	"strings"
)

func GetHashFromHeader(h http.Header) string {
	digest := h.Get("digest")
	if len(digest) <= 4 {
		return ""
	}
	if digest[:7] != "sha256=" {
		return ""
	}
	return digest[7:]
}
func GetSizeFromHeader(h http.Header) int64 {
	size, _ := strconv.ParseInt(h.Get("content-length"), 0, 64)
	return size
}
func GetOffsetFromHeader(h http.Header) int64 {
	byteRange := h.Get("range")
	if len(byteRange) < 7 {
		return 0
	}
	if byteRange[:6] != "bytes=" {
		return 0
	}
	bytePos := strings.Split(byteRange[6:], "-")
	offset, _ := strconv.ParseInt(bytePos[0], 0, 64)
	return offset
}
