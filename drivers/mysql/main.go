package main

import (
	"github.com/datazip-inc/olake"
	"github.com/datazip-inc/olake/drivers/base"
	driver "github.com/datazip-inc/olake/drivers/mysql/internal"
	"github.com/datazip-inc/olake/protocol"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	driver := &driver.MySQL{
		Driver: base.NewBase(),
	}
	_ = protocol.ChangeStreamDriver(driver)

	defer driver.Close()
	olake.RegisterDriver(driver)
}
