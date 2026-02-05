package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/elasticsearch/internal"
)

func main() {
	driver := &driver.Elasticsearch{
		CDCSupport: false,
	}
	defer driver.Close()
	olake.RegisterDriver(driver)
}
