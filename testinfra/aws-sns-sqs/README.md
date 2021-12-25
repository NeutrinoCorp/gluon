# Running a local Amazon Web Services stack

We use `localstack` to interact with both SNS and SQS services. Nevertheless, as 
`localstack` has a _Pro_ version, features like _AWS Glue schema registry_ are not available for free.

Thus, the registry MUST be created in your actual _AWS Account_ using Terraform or manually. *

*If using Apache Avro.

You MUST create each SNS topic, SQS queue and SNS subscription to SQS queues.
At this moment, no IAM roles are required for local live infrastructure.

A localstack container is provided on the `docker-compose.yml` file for faster development.

You can either use _Terraform_ with _localstack_ configured to call local endpoints or
use the _AWS CLI_.

For more reference about Terraform integration with localstack, please read [this article](https://docs.localstack.cloud/integrations/terraform/).


## Create an SNS topic

```console
ncorp@workstation:~$ aws --endpoint-url=http://localhost:4566 sns create-topic --name example-topic
```

## Create an SQS queue

```console
ncorp@workstation:~$ aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name example-queue
```

## Create an SNS to SQS subscription (topic-queue chaining)

```console
ncorp@workstation:~$ aws --endpoint-url=http://localhost:4566 sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:example-topic --protocol sqs --notification-endpoint http://localhost:4566/000000000000/example-queue
```

## Configuring Gluon to use Localstack

This configuration will redirect request only for Amazon SNS & SQS. Functionalities like Amazon Glue
schemas will still use actual cloud live infrastructure.

To replace completely AWS with localstack, please read the following [article](https://docs.localstack.cloud/integrations/sdks/go/)

_Note: If replacing AWS with localstack, AWS Glue is required which is a Pro feature from localstack._

```go
func newGluonBus() *gluon.Bus {
	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	cfg, _ := config.LoadDefaultConfig(context.TODO())
	bus := gluon.NewBus("aws_sns_sqs",
            gluon.WithSchemaRegistry(gaws.GlueSchemaRegistry{
                Client:           glue.NewFromConfig(cfg),
                RegistryName:     "sample",
                UseLatestVersion: true,
            }),
            gluon.WithDriverConfiguration(gaws.SnsSqsConfig{
                AwsConfig: cfg,
                SnsClient: sns.NewFromConfig(cfg, func(options *sns.Options) {
                    options.EndpointResolver = sns.EndpointResolverFunc(func(region string, options sns.EndpointResolverOptions) (aws.Endpoint, error) {
                        return aws.Endpoint{
                            PartitionID:   "aws",
                            URL:           awsEndpoint,
                            SigningRegion: awsRegion,
                        }, nil
                    })
                }),
                SqsClient: sqs.NewFromConfig(cfg, func(options *sqs.Options) {
                    options.EndpointResolver = sqs.EndpointResolverFunc(func(region string, options sqs.EndpointResolverOptions) (aws.Endpoint, error) {
                        return aws.Endpoint{
                            PartitionID:   "aws",
                            URL:           awsEndpoint,
                            SigningRegion: awsRegion,
                        }, nil
                    })
                }),
                CustomSqsEndpoint:    awsEndpoint,
                AccountID:            "000000000000",
                FailedPollingBackoff: time.Second * 5}))
	return bus
}
```
