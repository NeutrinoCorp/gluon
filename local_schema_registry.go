package gluon

import (
	"os"
	"path/filepath"
)

type LocalSchemaRegistry struct {
	BasePath string
}

var _ SchemaRegistry = LocalSchemaRegistry{}

func (l LocalSchemaRegistry) GetBaseLocation() string {
	return l.BasePath
}

func (l LocalSchemaRegistry) IsUsingLatestSchema() bool {
	return true
}

func (l LocalSchemaRegistry) GetSchemaDefinition(schemaName string, _ int) (string, error) {
	if l.BasePath == "" || schemaName == "" {
		return "", ErrMissingSchemaDefinition
	}

	path := l.BasePath + schemaName
	if path == "" {
		return "", ErrMissingSchemaDefinition
	}

	schemaBuf, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	return string(schemaBuf), nil
}
