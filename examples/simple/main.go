package main

import (
	"context"
	"fmt"
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
	true as test_bool,
	5.4 as test_double,
	CAST('X' as char) as test_char
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
	TestDouble                float64   `athenaconv:"test_double"`
	TestChar                  rune      `athenaconv:"test_char"`
}

func main() {
	ctx := context.Background()
	// test code for reflect:
	// model := new(MyModel)
	// fmt.Printf("%+v\n", *model)

	// v := reflect.New(reflect.TypeOf(*model))
	// fmt.Printf("%+v\n", v.Elem())
	// fmt.Printf("%+v\n", v.Elem().Interface())
	// fmt.Printf("NumField: %d\n", v.Elem().NumField())

	// modelType2 := reflect.TypeOf(new(MyModel)).Elem()
	// fmt.Printf("NumField: %d\n", modelType2.NumField())
	// for i := 0; i < modelType2.NumField(); i++ {
	// 	field := modelType2.Field(i)
	// 	fmt.Printf("Tag: %v\n", field.Tag)
	// 	fmt.Printf("athenaconv tag: %v\n", field.Tag.Get("athenaconv"))
	// }

	// modelType2 := reflect.TypeOf(MyModel{})
	// newModel2 := reflect.New(modelType2)
	// fmt.Printf("A: %+v\n", newModel2)
	// fmt.Printf("B: %+v\n", newModel2.Elem())
	// fmt.Printf("C: %+v\n", newModel2.Elem().Interface())
	// fmt.Printf("D: %+v\n", newModel2.Elem().NumField())

	// newModel2.Elem().FieldByName("ID").Set(reflect.ValueOf(-1))
	// fmt.Printf("E: %+v\n", newModel2.Elem())

	// // model2.Elem().Elem().FieldByName("ID").Set(reflect.ValueOf(5))
	// // fmt.Printf("Tag: %+v\n", model2)

	// return

	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	// awsConfig, err := config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile("temp1"), config.WithRegion(region))
	// awsConfig, err := config.LoadSharedConfigProfile(ctx, "temp1")
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
	fmt.Println("msg", "StartQueryExecution result", "QueryExecutionId", *startQueryExecOutput.QueryExecutionId)

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
			fmt.Println("msg", "stopped awaiting query results", "state", state)
			break
		}
		fmt.Println("msg", "still awaiting query results", "state", state, "waitTime", waitInterval)
		time.Sleep(waitInterval)
	}

	// TODO
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
			printOutput(queryResultOutput)

			// skip header row if first page results
			if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
				queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]
			}

			// TODO
			castedResultSet, err := mapper.FromAthenaResultSet(ctx, queryResultOutput.ResultSet)
			if err != nil {
				handleError(err)
			}
			output = append(output, castedResultSet[:]...)

			nextToken = queryResultOutput.NextToken
			if nextToken == nil {
				fmt.Println("msg", "finished fetching results from athena")
				break
			}
			fmt.Println("msg", "fetching next page results from athena", "nextToken", *nextToken)
			page++
		}
	} else {
		err = fmt.Errorf("query execution failed with status: %s", state)
		handleError(err)
	}

	for i, v := range output {
		fmt.Printf("final output index %d: %+v\n", i, v)
	}
}

func handleError(err error) {
	panic(err)
}

func printOutput(output *athena.GetQueryResultsOutput) {
	nextToken := ""
	if output.NextToken != nil {
		nextToken = *output.NextToken
	}
	fmt.Println("NextToken", nextToken)

	// print metadata:
	fmt.Println("RESULT SET METADATA")
	for _, columnInfo := range output.ResultSet.ResultSetMetadata.ColumnInfo {
		fmt.Println(*columnInfo.Name, "type:", *columnInfo.Type)
	}
	fmt.Println("END RESULT SET METADATA")

	// print data rows:
	fmt.Println("RESULT SET ROWS")
	for _, row := range output.ResultSet.Rows {
		for colIndex, col := range row.Data {
			fmt.Println("index", colIndex, "VarCharValue", *col.VarCharValue)
		}
	}
	fmt.Println("END RESULT SET ROWS")
}
