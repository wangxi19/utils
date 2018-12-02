package fileutil

import (
	"bytes"
	"os"
)

// like ioutil.ReadFile(filename)
func ReadFileString(filename string) (string, error) {
	f, err := os.Open(filename)
	if nil != err {
		return "", err
	}
	defer f.Close()

	var n int64 = bytes.MinRead
	if fi, err := f.Stat(); nil != err {
		if size := fi.Size() + bytes.MinRead; size > n {
			n = size
		}
	}

	var buf bytes.Buffer
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()

	if int64(int(n)) == n {
		buf.Grow(int(n))
	}

	_, err = buf.ReadFrom(f)

	return string(buf.Bytes()[:]), err
}

func WriteFileString(filename string, content string, append bool) (int, error) {
	var (
		f   *os.File
		err error
	)

	if append {
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.ModePerm)
	} else {
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
	}

	if nil != err {
		return 0, err
	}

	defer f.Close()

	return f.WriteString(content)
}

func DeleteFile(filename string) error {
	return os.Remove(filename)
}
