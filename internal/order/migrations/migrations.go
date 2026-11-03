package migrations

import "embed"

// FS contains the order service migrations.
//
//go:embed *.sql
var FS embed.FS
