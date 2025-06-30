# Fideo Readme

This was forked off the original Clickhouse Kafka Connect Sink project. 
The original intention was to be able to create a version that supports the AWS Glue Schema Registry. 

## Deployment
To deploy this connector build the shadow jar and upload it to S3. 
Then update the [Terraform for clickhouse_plugin](https://github.com/fideo-ai/devops/blob/main/src/terraform/environments/fideo-prod/msk-connect.tf#L272) to point to the new jar. 

```bash
./gradlew clean shadowJar
```

```bash
s3 aws cp build/libs/clickhouse-kafka-connect-*-all.jar s3://fideo-internal/engineering/jars/
```