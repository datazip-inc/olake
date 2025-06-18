package main

import (
	"fmt"

	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/postgres/internal"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	fmt.Print("test")
	driver := &driver.Postgres{
		CDCSupport: false,
	}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
