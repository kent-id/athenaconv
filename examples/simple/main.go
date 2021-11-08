package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv"
	"github.com/kent-id/athenaconv/util"
)

const (
	maxAllowedPageSize = 1000 // max allowed by athena
	region             = "us-east-1"
	workgroup          = "datalab"
	catalog            = "AwsDataCatalog"
	database           = "datalab"
	waitInterval       = 1 * time.Second
	testSQL            = `
select
    cc.compliance_computer_id as id,
	cc.name as name,
	count(*) as source_computers_count,
	array_agg(link.external_id order by link.external_id) as source_computer_ids,
	array_agg(ic.name order by ic.name) as source_computer_names,
	timestamp '2012-10-31 08:11:22' as test_timestamp,
	max(cc.inventory_date) as another_timestamp,
	date '2021-12-31' as test_date,
	true as test_bool,
	null as nil_value1,
	0 as zero_value1,
	null as nil_value2,
	null as nil_value3,
	null as nil_value4
from fnms_compliance_computers_view cc
join fnms_compliance_computer_connections link
	on link.org_id = cc.org_id and link.compliance_computer_id = cc.compliance_computer_id
join fnms_imported_computers_view ic
	on ic.org_id = link.org_id and ic.connection_id = link.connection_id and ic.external_id = link.external_id
where cc.org_id = 27826
group by cc.compliance_computer_id, cc.name
having count(*) > 1
order by cc.compliance_computer_id
limit 1
`
)

// MyModel defines a schema that corresponds with your testSQL above
type MyModel struct {
	ID                        *int       `athenaconv:"id"`
	Name                      *string    `athenaconv:"name"`
	SourceComputersCount      *int64     `athenaconv:"source_computers_count"`
	SourceComputerExternalIDs []string   `athenaconv:"source_computer_ids"`
	SourceComputerNames       []string   `athenaconv:"source_computer_names"`
	TestTimestamp             time.Time  `athenaconv:"test_timestamp"`
	AnotherTimestamp          *time.Time `athenaconv:"another_timestamp"`
	TestDate                  time.Time  `athenaconv:"test_date"`
	TestBool                  *bool      `athenaconv:"test_bool"`
	NilValue1                 *int64     `athenaconv:"nil_value1"`
	ZeroValue1                *int       `athenaconv:"zero_value1"`
	NilValue2                 *int8      `athenaconv:"nil_value2"`
	NilValue3                 *time.Time `athenaconv:"nil_value3"`
	NilValue4                 *string    `athenaconv:"nil_value4"`
}

func main() {
	// set logLevel for athenaconv, default is WARN
	athenaconv.SetLogLevel(athenaconv.LogLevelDebug)

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
		QueryString:           util.RefString(testSQL),
	}

	startQueryExecOutput, err := awsAthenaClient.StartQueryExecution(ctx, &startQueryExecInput)
	if err != nil {
		handleError(err)
	}

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
		log.Println("msg", "still awaiting query results", "state", state, "waitInterval", waitInterval)
		time.Sleep(waitInterval)
	}

	var result []MyModel // final result containing athena results from all pages

	// 3. finally if query is successful, get the query results output
	if state == types.QueryExecutionStateSucceeded {
		queryResultInput := athena.GetQueryResultsInput{
			QueryExecutionId: startQueryExecOutput.QueryExecutionId,
			MaxResults:       util.RefInt32(maxAllowedPageSize),
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

			var output []MyModel
			err := athenaconv.ConvertResultSetV2(ctx, &output, queryResultOutput.ResultSet)
			if err != nil {
				handleError(err)
			}
			log.Println("finished mapping, len(output) =", len(output))
			result = append(result, output...)

			nextToken = queryResultOutput.NextToken
			if nextToken == nil {
				log.Println("msg", "finished fetching results from athena")
				break
			}

			page++
			log.Println("msg", "fetching next page results from athena", "nextToken", *nextToken, "nextPage", page)
		}
	} else {
		reason := util.SafeString(queryExecOutput.QueryExecution.Status.StateChangeReason)
		err = fmt.Errorf("query execution failed with status: %s, reason: %s", state, reason)
		handleError(err)
	}

	log.Println("FINAL OUTPUT below, len =", len(result))
	for i, v := range result {
		log.Printf("index %d: %+v\n", i, v)
	}
}

func handleError(err error) {
	panic(err)
}
