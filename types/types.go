package types

// The metadata and rows that comprise a query result set. The metadata describes
// the column structure and data types. To return a ResultSet object, use
// GetQueryResults.
type ResultSet struct {
	// The metadata that describes the column structure and data types of a table of
	// query results.
	ResultSetMetadata *ResultSetMetadata

	// The rows in the table.
	Rows []Row
}

// The metadata that describes the column structure and data types of a table of
// query results. To return a ResultSetMetadata object, use GetQueryResults.
type ResultSetMetadata struct {

	// Information about the columns returned in a query result metadata.
	ColumnInfo []ColumnInfo
}

// Information about the columns in a query execution result.
type ColumnInfo struct {

	// The name of the column.
	//
	// This member is required.
	Name *string

	// The data type of the column.
	//
	// This member is required.
	Type *string

	// Indicates whether values in the column are case-sensitive.
	CaseSensitive bool

	// The catalog to which the query results belong.
	CatalogName *string

	// A column label.
	Label *string

	// Indicates the column's nullable status.
	// Nullable ColumnNullable

	// For DECIMAL data types, specifies the total number of digits, up to 38. For
	// performance reasons, we recommend up to 18 digits.
	Precision int32

	// For DECIMAL data types, specifies the total number of digits in the fractional
	// part of the value. Defaults to 0.
	Scale int32

	// The schema name (database name) to which the query results belong.
	SchemaName *string

	// The table name for the query results.
	TableName *string
}

// The rows that comprise a query result table.
type Row struct {
	// The data that populates a row in a query result table.
	Data []Datum
}

// A piece of data (a field in the table).
type Datum struct {
	// The value of the datum.
	VarCharValue *string
}
