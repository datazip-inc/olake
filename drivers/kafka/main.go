package main

import (
	"context"

	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/kafka/internal"
)

func main() {
	driver := &driver.Kafka{}
	defer driver.Close(context.Background())
	olake.RegisterDriver(driver)
}
