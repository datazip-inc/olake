package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/google-sheets/internal"
)

func main() {
	driver := &driver.GoogleSheets{}
	olake.RegisterDriver(driver)
}
