![logo](https://repository-images.githubusercontent.com/410152783/12feee34-e08d-4725-ae11-d5109a7a56aa "athenaconv logo")

Provides conversion from athena outputs to strongly defined data models.

[![Build and test](https://github.com/kent-id/athenaconv/actions/workflows/athenaconv.yaml/badge.svg)](https://github.com/kent-id/athenaconv/actions/workflows/athenaconv.yaml) [![Coverage Status](https://coveralls.io/repos/github/kent-id/athenaconv/badge.svg)](https://coveralls.io/github/kent-id/athenaconv)

## Getting started
Given the following data struct you define:

```go
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
```

And the following sql:
```sql
select
    id,
    name,
    count(source_id) as source_computers_count,
    array_agg(source_id) as source_computer_ids,
    array_agg(source_name) as source_computer_names,
    timestamp '2012-10-31 08:11:22' as test_timestamp,
    date '2021-12-31' as test_date,
    true as test_bool
from my_glue_catalog_table
group by id, name
```

You can convert your `athena.GetQueryResultOutput` object to strongly-typed `dest`:

**`dest` being a slice:**

```go
var dest []MyModel 
mapper, err := athenaconv.NewMapperFor(&dest)
err = dest.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
```

**`dest` being a channel:**

```go
dest := make(chan MyModel)
mapper, err := athenaconv.NewMapperFor(dest)
err = dest.AppendResultSetV2(ctx, queryResultOutput.ResultSet)
```

**one time conversion without creating data mapper:**

```go
var dest []MyModel 
err := athenaconv.ConvertResultSetV2(ctx, &dest, queryResultOutput.ResultSet)
```

## Supported data types
See [conversion.go](https://github.com/kent-id/athenaconv/blob/main/conversion.go) in this repo and [supported data types in athena](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) for more details.

| Athena data type                         | Go data type                         | Comments                                                                  |
| :--------------------------------------- | :----------------------------------- | :------------------------------------------------------------------------ |
| varchar                                  | string                               |                                                                           |
| boolean                                  | bool                                 |                                                                           |
| integer                                  | int/int32                            |                                                                           |
| bigint                                   | int64                                |                                                                           |
| timestamp                                | time.Time                            |                                                                           |
| date                                     | time.Time                            |                                                                           |
| array                                    | []string                             | Individual items within array should not contain comma                    |
| other data types                         | string                               | Other data types currently unsupported, default to string (no conversion) |

## Supported AWS SDK version
- [github.com/aws/aws-sdk-go-v2/service/athena/types](https://github.com/aws/aws-sdk-go-v2/tree/main/service/athena/types)

## Roadmap / items to review
- [ ] Add more data type support in conversion.go
