package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/db2/internal"
)

func main() {
	driver := &driver.DB2{}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
