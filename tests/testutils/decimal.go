package testutils

// Decimal is an expected decimal column value: its exact text at the column's scale, e.g. "123.4500" for a
// decimal(18,4). The verifiers compare a destination decimal by that text, whatever form Spark returns it in.
type Decimal string
