package gluon

import (
	"strconv"
	"strings"
	"sync"
)

type schemaRegistryCachingMiddleware struct {
	next    SchemaRegistry
	mu      sync.RWMutex
	records map[string]string
}

var _ SchemaRegistry = &schemaRegistryCachingMiddleware{}

func (s *schemaRegistryCachingMiddleware) GetBaseLocation() string {
	return s.next.GetBaseLocation()
}

func (s *schemaRegistryCachingMiddleware) IsUsingLatestSchema() bool {
	return s.next.IsUsingLatestSchema()
}

func (s *schemaRegistryCachingMiddleware) GetSchemaDefinition(schemaName string, version int) (def string, err error) {
	cachingKey := strings.Join([]string{schemaName, strconv.Itoa(version)}, "#")
	internalDef, ok := s.records[cachingKey]
	if ok {
		def = internalDef
		return
	}
	defer func() {
		if err == nil {
			s.records[cachingKey] = def
		}
	}()
	def, err = s.next.GetSchemaDefinition(schemaName, version)
	return
}
