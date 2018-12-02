package fileutil

import (
	"bytes"
	"io"
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

func CopyFile(filename string, destfilename string) error {
	fdest, err := os.Create(destfilename)

	if nil != err {
		return err
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0)

	if nil != err {
		fdest.Close()
		return err
	}

	var buf [1024]byte
	for {
		n, err := f.Read(buf[:])
		if nil != err && io.EOF != err {
			fdest.Close()
			f.Close()
			os.Remove(destfilename)
			return err
		}

		_, err2 := fdest.Write(buf[:n])

		if nil != err2 {
			fdest.Close()
			f.Close()
			os.Remove(destfilename)
			return err2
		}

		if io.EOF == err {
			break
		}
	}

	fdest.Close()
	f.Close()
	return nil
}

func IOWrite(b []byte, f *os.File) (int, error) {
	return f.Write(b)
}

func IOWriteToStdout(b []byte) (int, error) {
	return IOWrite(b, os.Stdout)
}
