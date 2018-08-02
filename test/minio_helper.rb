require 'fileutils'
module MinioHelper

  module_function

  def env(key)
    ENV["MEDUSA_STORAGE_TEST_MINIO_#{key}"]
  end

  def endpoint
    env('ENDPOINT') || 'http://localhost:9000'
  end

  def bucket
    ENV['BUCKET'] || 'medusa-storage'
  end

  def storage_directory
    env('STORAGE_DIR') || '~/minio'
  end

  def region
    env('REGION') || 'us-east-1'
  end

  def access_key
    env('ACCESS_KEY') || ''
  end

  def secret_key
    env('SECRET_KEY') || ''
  end

  def bucket_directory
    File.join(storage_directory, bucket)
  end

  def fixtures_directory
    File.join(File.dirname(__FILE__), 'fixtures')
  end

  def remove_fixtures
    FileUtils.rm_rf(bucket_directory)
  end

  def install_fixtures
    FileUtils.cp_r(fixtures_directory, bucket_directory)
  end

  def install_prefixed_fixtures(prefix)
    target_directory = File.join(bucket_directory, prefix)
    FileUtils.mkdir_p(File.dirname(target_directory))
    FileUtils.cp_r(fixtures_directory, target_directory)
  end

end