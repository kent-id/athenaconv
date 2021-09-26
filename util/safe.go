package util

// SafeString returns empty string if null
func SafeString(input *string) string {
	if input == nil {
		return ""
	}
	return *input
}

// SafeInt returns 0 if null
func SafeInt(input *int) int {
	if input == nil {
		return 0
	}
	return *input
}

// SafeInt32 returns 0 if null
func SafeInt32(input *int32) int32 {
	if input == nil {
		return 0
	}
	return *input
}

// SafeInt64 returns 0 if null
func SafeInt64(input *int64) int64 {
	if input == nil {
		return 0
	}
	return *input
}

// RefString returns a reference to a string
func RefString(input string) *string {
	return &input
}

// RefInt returns a reference to an int
func RefInt(input int) *int {
	return &input
}

// RefInt32 returns a reference to an int32
func RefInt32(input int32) *int32 {
	return &input
}

// RefUInt32 returns a reference to an int32
func RefUint32(input uint32) *uint32 {
	return &input
}

// RefInt64 returns a reference to an int64
func RefInt64(input int64) *int64 {
	return &input
}

// RefUint64 returns a reference to an uint64
func RefUint64(input uint64) *uint64 {
	return &input
}
