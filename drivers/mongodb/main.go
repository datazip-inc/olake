package main

import (
	"github.com/datazip-inc/olake"
	"github.com/datazip-inc/olake/drivers/base"
	driver "github.com/datazip-inc/olake/drivers/mongodb/internal"
	"github.com/datazip-inc/olake/protocol"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.Mongo{
		Driver: base.NewBase(),
	}
	defer driver.Close()

	_ = protocol.ChangeStreamDriver(driver)
	olake.RegisterDriver(driver)
}
