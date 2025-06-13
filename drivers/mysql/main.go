package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/mysql/internal"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.MySQL{
		CDCSupport: false,
	}
	defer driver.Close()
	olake.RegisterDriver(driver)
}
