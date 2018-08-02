## Minio usage

The tests assume there is a working minio
installation running on the local host. The general idea
is that there is a fixture bucket committed to source control.
Before each test run involving minio we copy this bucket into the storage of the
running minio to make it available, similar to the filesystem
tests. After each such test run we delete it. 

I think minio will function sufficiently properly if we do this, but
if not then we'll have to rig up something more sophisticated.

Some environment variables are available to control behavior here:

* MEDUSA_STORAGE_TEST_MINIO_ENDPOINT - default 'http://localhost:9000'
* MEDUSA_STORAGE_TEST_MINIO_BUCKET - default 'medusa-storage'
* MEDUSA_STORAGE_TEST_MINIO_STORAGE_DIR - default '~/minio'
* MEDUSA_STORAGE_TEST_MINIO_REGION - default 'us-east-1'
* MEDUSA_STORAGE_TEST_MINIO_ACCESS_KEY - default ''
* MEDUSA_STORAGE_TEST_MINIO_SECRET_KEY - default ''

