package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var Log *zap.Logger

func InitLogger() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.ConsoleSeparator = " | "
	config.EncoderConfig.StacktraceKey = "" // Disable stacktrace for cleaner logs

	// Use console encoder (colored) instead of json for local simulation readability
	consoleEncoder := zapcore.NewConsoleEncoder(config.EncoderConfig)
	
	core := zapcore.NewCore(
		consoleEncoder,
		zapcore.Lock(os.Stdout),
		zap.DebugLevel,
	)

	Log = zap.New(core)
}
