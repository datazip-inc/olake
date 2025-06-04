package main

import (
	"github.com/datazip-inc/olake"
	"github.com/datazip-inc/olake/drivers/base"
	driver "github.com/datazip-inc/olake/drivers/kafka/internal"
)

func main() {
	driver := &driver.Kafka{
		Driver: base.NewBase(),
	}
	olake.RegisterDriver(driver)
}
