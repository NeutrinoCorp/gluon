package gluon

import "errors"

var ErrMissingSchemaDefinition = errors.New("gluon: Missing schema definition")

type SchemaRegistry interface {
	GetBaseLocation() string
	IsUsingLatestSchema() bool
	GetSchemaDefinition(schemaName string, version int) (string, error)
}
