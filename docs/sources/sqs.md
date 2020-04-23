
# SQS Source

Credentials are taken by the aws sdk defaukt provider chain:
https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html 

```yaml
source:
  type: sqs
  
  # Location
  url: 
  region: us-east-1
  #endpoint: http://sqs:9324 useful for local testing

  # Options
  visibilityTimeoutInSeconds: 100 # defaults to 30
  maxDequeueCount: 1 # deaults to 5
``` 