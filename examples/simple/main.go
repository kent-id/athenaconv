package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv"
	"github.com/kent-id/athenaconv/util"
)

const (
	region       = "us-east-1"
	workgroup    = "datalab"
	catalog      = "AwsDataCatalog"
	database     = "datalab"
	waitInterval = 1 * time.Second
	testSql      = `
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
limit 5
`
)

// myModel defines a schema that corresponds with your testSql above
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
	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		handleError(err)
	}

	awsAthenaClient := athena.NewFromConfig(awsConfig)

	// 1. start query
	startQueryExecContext := types.QueryExecutionContext{
		Database: util.RefString(database),
		Catalog:  util.RefString(catalog),
	}

	startQueryExecInput := athena.StartQueryExecutionInput{
		QueryExecutionContext: &startQueryExecContext,
		WorkGroup:             util.RefString(workgroup),
		QueryString:           util.RefString(testSql),
	}

	startQueryExecOutput, err := awsAthenaClient.StartQueryExecution(ctx, &startQueryExecInput)
	if err != nil {
		handleError(err)
	}
	log.Println("msg", "StartQueryExecution result", "QueryExecutionId", *startQueryExecOutput.QueryExecutionId)

	// 2. get query execution info and wait until query finishes
	queryExecInput := athena.GetQueryExecutionInput{
		QueryExecutionId: startQueryExecOutput.QueryExecutionId,
	}

	var queryExecOutput *athena.GetQueryExecutionOutput
	var state types.QueryExecutionState

	for {
		queryExecOutput, err = awsAthenaClient.GetQueryExecution(ctx, &queryExecInput)
		if err != nil {
			handleError(err)
		}
		state = queryExecOutput.QueryExecution.Status.State
		if state != types.QueryExecutionStateRunning && state != types.QueryExecutionStateQueued {
			log.Println("msg", "stopped awaiting query results", "state", state)
			break
		}
		log.Println("msg", "still awaiting query results", "state", state, "waitTime", waitInterval)
		time.Sleep(waitInterval)
	}

	modelType := reflect.TypeOf(MyModel{})
	mapper, err := athenaconv.NewMapperFor(modelType)
	if err != nil {
		handleError(err)
	}
	output := make([]interface{}, 0)

	// 3. finally if query is successful, get the query results output
	if state == types.QueryExecutionStateSucceeded {
		queryResultInput := athena.GetQueryResultsInput{
			QueryExecutionId: startQueryExecOutput.QueryExecutionId,
			MaxResults:       util.RefInt32(3),
		}

		var queryResultOutput *athena.GetQueryResultsOutput
		var nextToken *string = nil
		var page uint = 1
		for {
			queryResultInput.NextToken = nextToken
			queryResultOutput, err = awsAthenaClient.GetQueryResults(ctx, &queryResultInput)
			if err != nil {
				handleError(err)
			}

			// skip header row if first page results
			if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
				queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]
			}

			castedResultSet, err := mapper.FromAthenaResultSet(ctx, queryResultOutput.ResultSet)
			if err != nil {
				handleError(err)
			}
			output = append(output, castedResultSet[:]...)

			nextToken = queryResultOutput.NextToken
			if nextToken == nil {
				log.Println("msg", "finished fetching results from athena")
				break
			}
			log.Println("msg", "fetching next page results from athena", "nextToken", *nextToken)
			page++
		}
	} else {
		err = fmt.Errorf("query execution failed with status: %s", state)
		handleError(err)
	}

	for i, v := range output {
		log.Printf("final output index %d: %+v\n", i, v)
	}
}

func handleError(err error) {
	panic(err)
}
