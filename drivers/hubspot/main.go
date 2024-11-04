package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/hubspot/internal"
)

func main() {
	driver := &driver.Hubspot{}
	olake.RegisterDriver(driver)
}
