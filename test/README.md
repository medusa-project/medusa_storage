## s3-server and rclone usage

The tests assume that you have docker available and have installed the scality/s3server:mem-latest container
using port 18000 with name medusa-storage-s3-server. E.g.

```
 docker run --name medusa-storage-s3-server -p 18000:8000 scality/s3server:mem-latest
```

Once installed the tests will restart this as necessary.

The tests also assume that you have rclone installed. An rclone config file is provided and
used that is compatible with the above docker setup.