package protocol

import (
	"bytes"
	"compress/flate"
	"io"
)

// deflateRaw compresses data using raw DEFLATE (no zlib/gzip header), which
// is what fflate's deflateSync/inflateSync produce on the Node.js side and
// what Python's zlib produces with wbits=-15. Go's compress/flate package is
// raw DEFLATE by default, so this is a direct wire-format match.
func deflateRaw(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// inflateRaw decompresses a raw DEFLATE stream produced by deflateRaw (or by
// fflate/zlib in the equivalent Node.js/Python implementations).
func inflateRaw(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}
