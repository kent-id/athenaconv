Provides conversion from athena outputs to strongly defined data models.

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
mapped, err := mapper.FromAthenaResultSet(ctx, queryResultOutput.ResultSet)
if err != nil {
    handleError(err)
}
```

## Supported data types
See conversion.go and [supported data types in athena](https://docs.aws.amazon.com/athena/latest/ug/data-types.html) for more details.
- boolean
- varchar
- integer
- bigint
- array
- timestamp
- date
- other athena data types default to string

## Known limitations
- Individual items within array data type cannot contain comma.