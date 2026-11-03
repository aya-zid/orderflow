package migrations

import "embed"

// FS contains the inventory service migrations.
//
//go:embed *.sql
var FS embed.FS
