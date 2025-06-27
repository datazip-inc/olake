package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/oracle/internal"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.Oracle{
		CDCSupport: false,
	}
	defer driver.Close()
	olake.RegisterDriver(driver)
}
