package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/postgres/internal"
)

func main() {
	driver := &driver.Postgres{
		CDCSupport: false,
	}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
