package main

import (
    "github.com/datazip-inc/olake"
    driver "github.com/datazip-inc/olake/drivers/elasticsearch/internal"
)

func main() {
    d := &driver.Elasticsearch{}
    defer d.Close()
    olake.RegisterDriver(d)
}
