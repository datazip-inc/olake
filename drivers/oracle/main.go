package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/oracle/internal"
	"github.com/datazip-inc/olake/utils/logger"
)

func main() {
	logger.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oracleDriver := &internal.Oracle{}
	absDriver := abstract.NewAbstractDriver(ctx, oracleDriver)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, cleaning up...")
		cancel()
	}()

	if err := absDriver.Run(ctx); err != nil {
		logger.Errorf("Failed to run Oracle driver: %v", err)
		os.Exit(1)
	}
}
