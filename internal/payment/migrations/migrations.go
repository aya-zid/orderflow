package migrations

import "embed"

// FS contains the payment service migrations.
//
//go:embed *.sql
var FS embed.FS
