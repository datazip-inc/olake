module github.com/datazip-inc/olake/drivers/elasticsearch

go 1.20

require (
    github.com/elastic/go-elasticsearch/v8 v8.6.0
    github.com/datazip-inc/olake v0.2.6
)

replace github.com/datazip-inc/olake => ../..
