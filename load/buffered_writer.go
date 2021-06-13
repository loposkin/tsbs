package load

import (
"bufio"
"os"
)

const (
	defaultWriteSize = 4 << 20 // 4 MB
)

// GetBufferedWriter returns the buffered Writer that should be used by the file writer
// if no file name is specified a buffer for STDIN is returned
func GetBufferedWriter(fileName string) *bufio.Writer {
	if len(fileName) == 0 {
		// Read from STDIN
		return bufio.NewWriterSize(os.Stdin, defaultReadSize)
	}
	// Read from specified file
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE,0644)
	if err != nil {
		fatal("cannot open file for write %s: %v", fileName, err)
		return nil
	}
	return bufio.NewWriterSize(file, defaultWriteSize)
}
