package athena

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/kent-id/athenaconv"
	"github.com/kent-id/athenaconv/util"
)

const (
	maxAllowedPageSize = 1000 // max allowed by athena
)

type athenaClientV2 struct {
	awsConfig    aws.Config
	workgroup    string
	catalog      string
	database     string
	waitInterval time.Duration
	maxPageSize  int32
}

// AthenaClientV2 is a client to AWS Athena providing strongly-typed model binding.
// Underlying AWS client from aws-sdk-go-v2 is used.
type AthenaClientV2 interface {
	GetQueryResults(ctx context.Context, sqlQuery string, dest interface{}) error
	GetQueryResultsIntoChannel(ctx context.Context, sqlQuery string, dest interface{}) error
}

// NewClientV2 constructs new AthenaClientV2 using specified aws-sdk-go-v2/aws/config, workgroup, database name, and catalog name in Athena
func NewClientV2(ctx context.Context, awsConfig aws.Config, workgroup, database, catalog string) AthenaClientV2 {
	athenaconv.LogInfof("creating athena client with workgroup: %s, database: %s, catalog: %s, pageSize: %d, config: %+v", workgroup, database, catalog, maxAllowedPageSize, awsConfig)
	return &athenaClientV2{
		awsConfig:    awsConfig,
		workgroup:    workgroup,
		catalog:      catalog,
		database:     database,
		waitInterval: 1 * time.Second,
		maxPageSize:  maxAllowedPageSize,
	}
}

// GetQueryResults gets query results for the given SQL query and outputs into dest slice.
//
// Example:
// var output []myStruct
// err := client.GetQueryResults(ctx, "select id from my_table", &output)
func (c *athenaClientV2) GetQueryResults(ctx context.Context, sqlQuery string, dest interface{}) error {
	// 1. first initiallize mapper which will also validate the dest model before we initiate query execution
	mapper, err := athenaconv.NewMapperFor(dest)
	if err != nil {
		return err
	}
	awsAthenaClient := athena.NewFromConfig(c.awsConfig)

	// 2. start query
	queryExecutionID, err := c.startQueryAndGetExecutionID(ctx, awsAthenaClient, sqlQuery)
	if err != nil {
		return err
	}

	// 3. get query execution info and wait until query finishes
	status, err := c.waitQueryAndGetStatus(ctx, awsAthenaClient, queryExecutionID)
	if err != nil {
		return err
	}

	// 4. finally if query is successful, get the query results output
	if status.State != types.QueryExecutionStateSucceeded {
		reason := util.SafeString(status.StateChangeReason)
		err = fmt.Errorf("query execution failed with status: %s, reason: %s", status.State, reason)
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

		// get results into dest and append into dest
		err = mapper.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
		if err != nil {
			return err
		}

		nextToken = queryResultOutput.NextToken
		if nextToken == nil {
			athenaconv.LogInfof("finished fetching results from athena")
			break
		}

		page++
		athenaconv.LogInfof("fetching next page %d results from athena using nextToken: %s", page, nextToken)
	}

	return nil
}

// GetQueryResultsIntoChannel gets query results for the given SQL query into dest channel.
func (c *athenaClientV2) GetQueryResultsIntoChannel(ctx context.Context, sqlQuery string, dest interface{}) error {
	// 1. first initiallize mapper which will also validate the dest model before we initiate query execution
	mapper, err := athenaconv.NewMapperFor(dest)
	if err != nil {
		return err
	}
	awsAthenaClient := athena.NewFromConfig(c.awsConfig)
	destChannel := reflect.ValueOf(dest)

	// 1. start query
	queryExecutionID, err := c.startQueryAndGetExecutionID(ctx, awsAthenaClient, sqlQuery)
	if err != nil {
		destChannel.Close()
		return err
	}

	// 2. get query execution info and wait until query finishes
	status, err := c.waitQueryAndGetStatus(ctx, awsAthenaClient, queryExecutionID)
	if err != nil {
		destChannel.Close()
		return err
	}

	// 3. finally if query is successful, get the query results output
	if status.State == types.QueryExecutionStateSucceeded {
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
				destChannel.Close()
				return err
			}

			// skip header row if first page results
			if page == 1 && len(queryResultOutput.ResultSet.Rows) > 0 {
				queryResultOutput.ResultSet.Rows = queryResultOutput.ResultSet.Rows[1:]
			}

			err = mapper.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
			if err != nil {
				destChannel.Close()
				return err
			}

			nextToken = queryResultOutput.NextToken
			if nextToken == nil {
				athenaconv.LogInfof("finished fetching results from athena")
				break
			}

			page++
			athenaconv.LogInfof("fetching next page %d results from athena using nextToken: %s", page, nextToken)
		}
	} else {
		reason := util.SafeString(status.StateChangeReason)
		err = fmt.Errorf("query execution failed with status: %s, reason: %s", status.State, reason)
		destChannel.Close()
		return err
	}
	destChannel.Close()
	return nil
}

// startQueryAndGetExecutionID starts query execution and get the execution id to identify the running query in Athena.
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
	athenaconv.LogInfof("started query with ExecutionID: %s", util.SafeString(startQueryExecOutput.QueryExecutionId))
	return startQueryExecOutput.QueryExecutionId, nil
}

// waitQueryAndGetStatus waits until query execution finishes and return QueryExecutionStatus.
func (c *athenaClientV2) waitQueryAndGetStatus(ctx context.Context, awsAthenaClient *athena.Client, queryExecutionID *string) (*types.QueryExecutionStatus, error) {
	queryExecInput := athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionID,
	}

	var status *types.QueryExecutionStatus
	for {
		queryExecOutput, err := awsAthenaClient.GetQueryExecution(ctx, &queryExecInput)
		if err != nil {
			return nil, err
		}
		status = queryExecOutput.QueryExecution.Status
		if status.State != types.QueryExecutionStateRunning && status.State != types.QueryExecutionStateQueued {
			athenaconv.LogInfof("stopped query execution with state: %s", status.State)
			break
		}
		athenaconv.LogInfof("still awaiting query resultswith state: %s, waitInterval: %s", status.State, c.waitInterval)
		time.Sleep(c.waitInterval)
	}
	return status, nil
}
