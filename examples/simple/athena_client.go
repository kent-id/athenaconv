package main

// import (
// 	"context"
// 	"fmt"
// 	"time"

// 	"github.com/aws/aws-sdk-go/aws"
// 	"github.com/aws/aws-sdk-go/aws/session"
// 	"github.com/aws/aws-sdk-go/service/athena"
// )

// type athenaClient struct {
// 	region       string
// 	workgroup    string
// 	catalog      string
// 	database     string
// 	waitInterval time.Duration
// }

// type AthenaClient interface {
// 	GetQueryResults(ctx context.Context, sqlQuery string) (*athena.GetQueryResultsOutput, error)
// }

// // NewAthenaClient constructs new AthenaClient implementation
// func NewAthenaClient(ctx context.Context, region, workgroup, database, catalog string) AthenaClient {
// 	//log.Debug(ctx, "msg", "awslibs.NewAthenaClient", "region", region, "workgroup", workgroup, "catalog", catalog, "database", database)
// 	return &athenaClient{
// 		region:       region,
// 		workgroup:    workgroup,
// 		catalog:      catalog,
// 		database:     database,
// 		waitInterval: 1 * time.Second,
// 	}
// }

// // GetQueryResults returns query results for the given SQL query
// func (c *athenaClient) GetQueryResults(ctx context.Context, sqlQuery string) (*athena.GetQueryResultsOutput, error) {
// 	awsConfig := &aws.Config{}
// 	awsConfig.WithRegion(c.region)
// 	awsSession := session.Must(session.NewSession(awsConfig))
// 	awsAthenaClient := athena.New(awsSession, aws.NewConfig().WithRegion(c.region))

// 	// 1. start query
// 	var startQueryExecContext athena.QueryExecutionContext
// 	startQueryExecContext.SetDatabase(c.database)
// 	startQueryExecContext.SetCatalog(c.catalog)

// 	var startQueryExecInput athena.StartQueryExecutionInput
// 	startQueryExecInput.SetQueryExecutionContext(&startQueryExecContext)
// 	startQueryExecInput.SetWorkGroup(c.workgroup)
// 	startQueryExecInput.SetQueryString(sqlQuery)

// 	startQueryExecOutput, err := awsAthenaClient.StartQueryExecution(&startQueryExecInput)
// 	if err != nil {
// 		return nil, err
// 	}
// 	//log.Info(ctx, "msg", "StartQueryExecution result", "result", startQueryExecOutput.GoString())

// 	// 2. get query execution info and wait until query finishes
// 	var queryExecInput athena.GetQueryExecutionInput
// 	queryExecInput.SetQueryExecutionId(*startQueryExecOutput.QueryExecutionId)

// 	var queryExecOutput *athena.GetQueryExecutionOutput

// 	for {
// 		queryExecOutput, err = awsAthenaClient.GetQueryExecution(&queryExecInput)
// 		if err != nil {
// 			return nil, err
// 		}
// 		state := queryExecOutput.QueryExecution.Status.State
// 		if *state != "RUNNING" && *state != "QUEUED" {
// 			//log.Debug(ctx, "msg", "stopped awaiting query results", "state", state)
// 			break
// 		}
// 		//log.Debug(ctx, "msg", "still awaiting query results", "state", state, "waitTime", c.waitInterval)
// 		time.Sleep(c.waitInterval)
// 	}

// 	// 3. finally if query is successful, get the query results output
// 	if *queryExecOutput.QueryExecution.Status.State == "SUCCEEDED" {
// 		var queryResultInput athena.GetQueryResultsInput
// 		queryResultInput.SetQueryExecutionId(*startQueryExecOutput.QueryExecutionId)

// 		var queryResultOutput *athena.GetQueryResultsOutput
// 		var nextToken *string
// 		for {
// 			queryResultInput.SetQueryExecutionId(*startQueryExecOutput.QueryExecutionId)
// 			queryResultInput.SetMaxResults(3)
// 			if nextToken != nil {
// 				queryResultInput.SetNextToken(*nextToken)
// 			}

// 			queryResultOutput, err = awsAthenaClient.GetQueryResults(&queryResultInput)
// 			if err != nil {
// 				return nil, err
// 			}
// 			nextToken = queryResultOutput.NextToken
// 			if nextToken == nil {
// 				//log.Debug(ctx, "msg", "finished fetching results from athena")
// 				log.Printf("%+v", queryResultOutput)
// 				break
// 			}
// 			//log.Debug(ctx, "msg", "fetching next page results from athena", "nextToken", nextToken)
// 			log.Printf("%+v", queryResultOutput)
// 		}
// 		return queryResultOutput, nil // TODO concat results
// 	} else {
// 		err = fmt.Errorf("query execution failed with status: %s", *queryExecOutput.QueryExecution.Status.State)
// 		return nil, err
// 	}
// }
