package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv"
	"github.com/kent-id/athenaconv/util"
)

type athenaClientV2 struct {
	awsConfig    aws.Config
	workgroup    string
	catalog      string
	database     string
	waitInterval time.Duration
	maxPageSize  int32
}

// AthenaClientV2 is a client to AWS Athena providing strongly-typed model binding and using aws-sdk-go-v2
type AthenaClientV2 interface {
	GetQueryResults(ctx context.Context, sqlQuery string, dest interface{}) error
	// GetQueryResultsIntoChannel(ctx context.Context, sqlQuery string, modelType reflect.Type, resultsChannel chan<- interface{}, errorsChan chan<- error)
}

// NewAthenaClientV2 constructs new AthenaClient implementation
func NewAthenaClientV2(ctx context.Context, awsConfig aws.Config, workgroup, database, catalog string) AthenaClientV2 {
	log.Println("msg", "awslibs.NewAthenaClient", "awsConfig", awsConfig, "workgroup", workgroup, "catalog", catalog, "database", database)
	return &athenaClientV2{
		awsConfig:    awsConfig,
		workgroup:    workgroup,
		catalog:      catalog,
		database:     database,
		waitInterval: 1 * time.Second,
		maxPageSize:  1000, // max allowed by athena
	}
}

// GetQueryResults returns query results for the given SQL query
// Example:
// var output []myStruct
// err := client.GetQueryResults(ctx, "select id from my_table", &output)
func (c *athenaClientV2) GetQueryResults(ctx context.Context, sqlQuery string, dest interface{}) error {
	// 1. first initiallize mapper which will also validate the dest model before we initiate query execution
	mapper, err := athenaconv.NewMapperFor(dest)
	if err != nil {
		return err
	}

	// // temp code
	// tmp := reflect.MakeSlice(reflect.SliceOf(modelType), 0, 0)
	// // tmp2 := reflect.New(reflect.TypeOf(tmp)).Interface()
	// modelType, err = athenaconv.GetModelType(&tmp)
	// if err != nil {
	// 	panic(err)
	// } else {
	// 	err = fmt.Errorf("all good - %s", modelType)
	// 	panic(err)
	// }

	awsAthenaClient := athena.NewFromConfig(c.awsConfig)

	// 2. start query
	queryExecutionID, err := c.startQueryAndGetExecutionID(ctx, awsAthenaClient, sqlQuery)
	if err != nil {
		return err
	}

	// 3. get query execution info and wait until query finishes
	state, err := c.waitQueryAndGetState(ctx, awsAthenaClient, queryExecutionID)
	if err != nil {
		return err
	}

	// 4. finally if query is successful, get the query results output
	if *state != types.QueryExecutionStateSucceeded {
		err = fmt.Errorf("query execution failed with status: %s", *state)
		return err
	}
	queryResultInput := athena.GetQueryResultsInput{
		QueryExecutionId: queryExecutionID,
		MaxResults:       &c.maxPageSize,
	}

	var nextToken *string = nil
	var page uint = 1
	for {
		queryResultInput.NextToken = nextToken
		queryResultOutput, err := awsAthenaClient.GetQueryResults(ctx, &queryResultInput)
		if err != nil {
			return err
		}

		// skip header row if first page results
		if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
			queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]
		}

		// // temp code
		// tmp := reflect.MakeSlice(reflect.SliceOf(modelType), 0, 0).Addr()
		// _, err = athenaconv.GetModelType(&tmp)
		// if err != nil {
		// 	panic(err)
		// }

		// get results into dest and append into dest
		err = mapper.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
		if err != nil {
			err = fmt.Errorf("err1 - %s", err.Error())
			return err
		}

		nextToken = queryResultOutput.NextToken
		if nextToken == nil {
			log.Println("msg", "finished fetching results from athena")
			break
		}

		page++
		log.Println("msg", "fetching next page results from athena", "nextToken", *nextToken, "nextPage", page)
	}

	return nil
}

// GetQueryResultsIntoChannel returns query results for the given SQL query into the results channel
// func (c *athenaClientV2) GetQueryResultsIntoChannel(ctx context.Context, sqlQuery string, modelType reflect.Type, resultsChannel chan<- interface{}, errorsChan chan<- error) {
// 	closeChannels := func() {
// 		close(resultsChannel)
// 		close(errorsChan)
// 	}
// 	awsAthenaClient := athena.NewFromConfig(c.awsConfig)

// 	// 1. start query
// 	queryExecutionID, err := c.startQueryAndGetExecutionID(ctx, awsAthenaClient, sqlQuery)
// 	if err != nil {
// 		errorsChan <- err
// 		closeChannels()
// 		return
// 	}

// 	// 2. get query execution info and wait until query finishes
// 	state, err := c.waitQueryAndGetState(ctx, awsAthenaClient, queryExecutionID)
// 	if err != nil {
// 		errorsChan <- err
// 		closeChannels()
// 		return
// 	}

// 	// 3. finally if query is successful, get the query results output
// 	if *state == types.QueryExecutionStateSucceeded {
// 		queryResultInput := athena.GetQueryResultsInput{
// 			QueryExecutionId: queryExecutionID,
// 			MaxResults:       &c.maxPageSize,
// 		}

// 		mapper, err := athenaconv.NewMapperFor(modelType)
// 		if err != nil {
// 			errorsChan <- err
// 			closeChannels()
// 			return
// 		}

// 		var nextToken *string = nil
// 		var page uint = 1
// 		for {
// 			queryResultInput.NextToken = nextToken
// 			queryResultOutput, err := awsAthenaClient.GetQueryResults(ctx, &queryResultInput)
// 			if err != nil {
// 				errorsChan <- err
// 				closeChannels()
// 				return
// 			}

// 			// skip header row if first page results
// 			if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
// 				queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]
// 			}

// 			mapped, err := mapper.FromAthenaResultSetV2(ctx, queryResultOutput.ResultSet)
// 			if err != nil {
// 				errorsChan <- err
// 				return
// 			}

// 			for _, mappedItem := range mapped {
// 				mappedItemModel := mappedItem
// 				resultsChannel <- mappedItemModel
// 			}

// 			nextToken = queryResultOutput.NextToken
// 			if nextToken == nil {
// 				log.Println("msg", "finished fetching results from athena")
// 				break
// 			}

// 			page++
// 			log.Println("msg", "fetching next page results from athena", "nextToken", *nextToken, "nextPage", page)
// 		}
// 	} else {
// 		err = fmt.Errorf("query execution failed with status: %s", *state)
// 		errorsChan <- err
// 		closeChannels()
// 		return
// 	}
// 	closeChannels()
// }

func (c *athenaClientV2) startQueryAndGetExecutionID(ctx context.Context, awsAthenaClient *athena.Client, sqlQuery string) (*string, error) {
	startQueryExecContext := types.QueryExecutionContext{
		Database: util.RefString(c.database),
		Catalog:  util.RefString(c.catalog),
	}

	startQueryExecInput := athena.StartQueryExecutionInput{
		QueryExecutionContext: &startQueryExecContext,
		WorkGroup:             util.RefString(c.workgroup),
		QueryString:           util.RefString(sqlQuery),
	}

	startQueryExecOutput, err := awsAthenaClient.StartQueryExecution(ctx, &startQueryExecInput)
	if err != nil {
		return nil, err
	}
	log.Println("msg", "StartQueryExecution result", "result", util.SafeString(startQueryExecOutput.QueryExecutionId))
	return startQueryExecOutput.QueryExecutionId, nil
}

func (c *athenaClientV2) waitQueryAndGetState(ctx context.Context, awsAthenaClient *athena.Client, queryExecutionID *string) (*types.QueryExecutionState, error) {
	queryExecInput := athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionID,
	}

	var state types.QueryExecutionState
	for {
		queryExecOutput, err := awsAthenaClient.GetQueryExecution(ctx, &queryExecInput)
		if err != nil {
			return nil, err
		}
		state = queryExecOutput.QueryExecution.Status.State
		if state != types.QueryExecutionStateRunning && state != types.QueryExecutionStateQueued {
			log.Println("msg", "stopped awaiting query results", "state", state)
			break
		}
		log.Println("msg", "still awaiting query results", "state", state, "waitTime", c.waitInterval)
		time.Sleep(c.waitInterval)
	}
	return &state, nil
}
