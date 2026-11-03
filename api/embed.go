// Package api embeds the OpenAPI contract so the gateway can serve it without
// a separate file server.
package api

import _ "embed"

// OpenAPISpec is the raw OpenAPI 3 document.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
