package gaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/neutrinocorp/gluon"
)

type GlueSchemaRegistry struct {
	Client           *glue.Client
	RegistryName     string
	UseLatestVersion bool
}

var _ gluon.SchemaRegistry = GlueSchemaRegistry{}

func (g GlueSchemaRegistry) GetBaseLocation() string {
	return g.RegistryName
}

func (g GlueSchemaRegistry) IsUsingLatestSchema() bool {
	return g.UseLatestVersion
}

func (g GlueSchemaRegistry) GetSchemaDefinition(schemaName string, version int) (string, error) {
	if g.UseLatestVersion {
		version = 0
	}
	schema, err := g.Client.GetSchemaVersion(context.TODO(), &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(g.RegistryName),
			SchemaName:   aws.String(schemaName),
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			LatestVersion: g.UseLatestVersion,
			VersionNumber: int64(version),
		},
	})
	if err != nil {
		return "", err
	} else if schema.SchemaDefinition == nil {
		return "", gluon.ErrMissingSchemaDefinition
	}

	return *schema.SchemaDefinition, nil
}
