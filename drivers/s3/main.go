package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/s3/internal"
)

func main() {
	driver := &driver.S3{}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
