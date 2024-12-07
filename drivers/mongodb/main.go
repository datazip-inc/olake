package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/mongodb/internal"
	"github.com/datazip-inc/olake/protocol"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.Mongo{}
	defer driver.Close()

	_ = protocol.ChangeStreamDriver(driver)
	olake.RegisterDriver(driver)
}
