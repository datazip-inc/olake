package main

import (
	"github.com/datazip-inc/olake"
	"github.com/datazip-inc/olake/drivers/base"
	driver "github.com/datazip-inc/olake/drivers/postgres/internal"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.Postgres{
		Driver: base.NewBase(),
	}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
