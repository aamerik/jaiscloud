package services

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeAWSChunked(t *testing.T) {
	t.Run("single chunk", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("chunk with signature extension", func(t *testing.T) {
		body := []byte("5;chunk-signature=abc123\r\nhello\r\n0;chunk-signature=def456\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("multiple chunks", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello world"), decodeAWSChunked(body))
	})

	t.Run("plain body passthrough", func(t *testing.T) {
		body := []byte("just plain text, not chunked")
		assert.Equal(t, body, decodeAWSChunked(body))
	})

	t.Run("empty body", func(t *testing.T) {
		assert.Equal(t, []byte(nil), decodeAWSChunked([]byte{}))
	})

	t.Run("hex chunk size 0x100 = 256 bytes", func(t *testing.T) {
		data := make([]byte, 256)
		for i := range data {
			data[i] = byte('a' + i%26)
		}
		// Build: "100\r\n<256 bytes>\r\n0\r\n\r\n"
		chunk := append([]byte("100\r\n"), data...)
		chunk = append(chunk, []byte("\r\n0\r\n\r\n")...)
		assert.Equal(t, data, decodeAWSChunked(chunk))
	})
}
