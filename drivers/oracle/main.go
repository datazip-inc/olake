package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/oracle/internal"
)

func main() {
	driver := &driver.Oracle{
		CDCSupport: false,
	}
	defer driver.Close()
	olake.RegisterDriver(driver)
}
