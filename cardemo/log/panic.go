package log

import (
	"os"
	"syscall"
)

func handlePanicLoggingWithFile(logFile *os.File) {
	syscall.Dup2(int(logFile.Fd()), syscall.Stderr)
}
