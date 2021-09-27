package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

// MyModel defines a schema that corresponds with your testSQL above
type MyModel struct {
	ID                        int       `athenaconv:"id"`
	Name                      string    `athenaconv:"name"`
	SourceComputersCount      int64     `athenaconv:"source_computers_count"`
	SourceComputerExternalIDs []string  `athenaconv:"source_computer_ids"`
	SourceComputerNames       []string  `athenaconv:"source_computer_names"`
	TestTimestamp             time.Time `athenaconv:"test_timestamp"`
	TestDate                  time.Time `athenaconv:"test_date"`
	TestBool                  bool      `athenaconv:"test_bool"`
}

func main() {
	// test code to get results from athena
	ctx := context.Background()

	sql := `
select
    cc.compliance_computer_id as id,
	cc.name as name,
	count(*) as source_computers_count,
	array_agg(link.external_id order by link.external_id) as source_computer_ids,
	array_agg(ic.name order by ic.name) as source_computer_names,
	timestamp '2012-10-31 08:11:22' as test_timestamp,
	date '2021-12-31' as test_date,
	true as test_bool
from xxx
group by cc.compliance_computer_id, cc.name
having count(*) > 1
order by cc.compliance_computer_id
limit 5
	`
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		handleError(err)
	}

	client := NewAthenaClientV2(ctx, awsConfig, "datalab", "datalab", "AwsDataCatalog")

	var wg sync.WaitGroup
	resultsChan := make(chan interface{})
	errorsChan := make(chan error)

	wg.Add(2)
	go func() {
		for result := range resultsChan {
			nextRow := result.(*MyModel)
			log.Println("msg", "received next row data", "data", fmt.Sprintf("%+v", nextRow))
		}
		wg.Done()
	}()
	go func() {
		for err := range errorsChan {
			handleError(err)
		}
		wg.Done()
	}()

	client.GetQueryResultsIntoChannel(ctx, sql, reflect.TypeOf(MyModel{}), resultsChan, errorsChan)
	wg.Wait()

	log.Println("program finished")
}

func handleError(err error) {
	panic(err)
}
