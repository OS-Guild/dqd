

```yaml
source:
  type: azure-queue
  
  # Location
  storageAccount: test
  queue: dqd
  #connection: http://azure:10001/devstoreaccount1 useful for local testing

  # Credentials
  storageAccountKey: ****
  sasToken: ****

  # Options
  visibilityTimeoutInSeconds: 100 # defaults to 60
  maxDequeueCount: 1 # deaults to 5
  retryVisiblityTimeoutInSeconds: [10, 500, 600]
``` 