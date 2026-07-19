package web

import _ "embed"

// indexHTML is the complete embedded UI: one self-contained document with no
// external network dependencies, so the worker daemon ships as one binary.
//
//go:embed static/index.html
var indexHTML []byte
