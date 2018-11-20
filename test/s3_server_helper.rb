module S3ServerHelper
  module_function

  def endpoint
    'http://localhost:18000'
  end

  def access_key
    'accessKey1'
  end

  def secret_key
    'verySecretKey1'
  end

  def region
    'us-east-1'
  end

  def setup_bucket_and_fixtures(bucket_name, prefix: nil, copy_fixtures: true, versioned: false)
    #credentials = Aws::Credentials.new(access_key, secret_key)
    #client = Aws::S3::Client.new(credentials: @credentials, endpoint: S3ServerHelper.endpoint, force_path_style: true, region: S3ServerHelper.region)
    #Sometimes I get an error creating the bucket in s3-server, coming from the AWS SDK:
    # Minitest::UnexpectedError: Seahorse::Client::NetworkingError: end of file reached
    # In this case, configure rclone and use it to create. I haven't seen problems for other operations.
    #@client.create_bucket(bucket: @bucket)
    system("rclone mkdir medusa-storage-s3-server:#{bucket_name}")
    if versioned
      credentials = Aws::Credentials.new(access_key, secret_key)
      client = Aws::S3::Client.new(credentials: credentials, endpoint: S3ServerHelper.endpoint, force_path_style: true, region: S3ServerHelper.region)
      client.put_bucket_versioning(bucket: bucket_name, versioning_configuration: {status: "Enabled"})
    end
    if copy_fixtures
      if prefix
        system("rclone copy #{fixture_location} medusa-storage-s3-server:#{bucket_name}/#{prefix}")
      else
        system("rclone copy #{fixture_location} medusa-storage-s3-server:#{bucket_name}")
      end
    end
  end

  def fixture_location
    File.join(File.dirname(__FILE__), 'fixtures')
  end

  def root_args(bucket_name, additional_args = {})
    {type: 's3', name: 's3', endpoint: S3ServerHelper.endpoint,
     bucket: bucket_name, region: S3ServerHelper.region,
     aws_access_key_id: S3ServerHelper.access_key,
     aws_secret_access_key: S3ServerHelper.secret_key,
     force_path_style: true}.merge(additional_args)
  end

end