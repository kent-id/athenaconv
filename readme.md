![logo](https://repository-images.githubusercontent.com/410152783/12feee34-e08d-4725-ae11-d5109a7a56aa "athenaconv logo")

Provides conversion from athena outputs to strongly defined data models.

The package works as intended but still needs more development:
- [ ] Add some more high-level tests around mapper.go
- [ ] Add more data type support in conversion.go
- [ ] Review usage of logging (best practice for logging in packages)
- [ ] Is referencing aws-sdk-go-v2 the best practice? How does that affect other client libraries referencing this package?

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

You can convert your `athena.GetQueryResultOutput` object to strongly-typed struct `MyModel` by doing this:

```go
modelType := reflect.TypeOf(MyModel{})
mapper, err := athenaconv.NewMapperFor(modelType)
if err != nil {
    handleError(err)
}

var mapped []interface{}
mapped, err := mapper.FromAthenaResultSetV2(ctx, queryResultOutput.ResultSet)
if err != nil {
    handleError(err)
}
```

## Supported data types
See [conversion.go](https://github.com/kent-id/athenaconv/blob/main/conversion.go) in this repo and [supported data types in athena](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) for more details.
- boolean
- varchar
- integer
- bigint
- array
- timestamp
- date
- other athena data types default to string

## Supported AWS SDK version
- github.com/aws/aws-sdk-go-v2/service/athena/types

## Known limitations
- Individual items within array data type cannot contain comma.
