package util

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
)

func ReadFile(path string) *bufio.Scanner {
	fullPath, e := filepath.Abs(path)
	if e != nil {
		LogErr("get full path err")
	}
	fdata, e := os.ReadFile(fullPath)
	if e != nil {
		LogErr("read file err")
	}
	r := bufio.NewReader(bytes.NewReader(fdata))
	return bufio.NewScanner(r)
}
