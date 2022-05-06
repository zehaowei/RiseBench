package util

import (
	"errors"
	"fmt"
)

const LogLevel int32 = 0

func Errorf(str string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(str, args...))
}

func LogInfo(str string, args ...interface{}) {
	fmt.Printf("[info] %s\n", fmt.Sprintf(str, args...))
}

func LogErr(str string, args ...interface{}) {
	fmt.Printf("[error] %s\n", fmt.Sprintf(str, args...))
}

func LogBench() {

}
