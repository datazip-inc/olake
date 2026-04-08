package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/mssql/internal"
)

func main() {
	driver := &driver.MSSQL{}
	defer driver.Close()
	olake.RegisterDriver(driver)
}

