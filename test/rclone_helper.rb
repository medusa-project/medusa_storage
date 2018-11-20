module RcloneHelper
  module_function

  def run(*args)
    system('rclone', *args)
  end

  def make_bucket(bucket_name)
    run('mkdir', bucket_string(bucket_name))
  end

  def bucket_string(bucket_name, prefix: nil)
    if prefix
      "#{server_name}:#{bucket_name}/#{prefix}"
    else
      "#{server_name}:#{bucket_name}"
    end
  end

  def copy_fixtures(fixture_location, bucket_name, prefix: nil)
    run('copy', fixture_location, bucket_string(bucket_name, prefix: prefix))
  end

  def server_name
    'medusa-storage-s3-server'
  end

end