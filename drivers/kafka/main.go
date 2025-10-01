package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/kafka/internal"
)

func main() {
	driver := &driver.Kafka{}
	defer driver.Close()
	olake.RegisterDriver(driver)
}
