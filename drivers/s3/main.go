package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/piyushsingariya/drivers/s3/internal"
)

func main() {
	driver := &driver.S3{}
	olake.RegisterDriver(driver)
}
