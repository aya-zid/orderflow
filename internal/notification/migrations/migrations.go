package migrations

import "embed"

// FS contains the notification service migrations.
//
//go:embed *.sql
var FS embed.FS
