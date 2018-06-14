#In this type of  MedusaStorage::Root the key is simply the key into the S3 storage bucket.
# Some additional methods relevant to S3 are provided.
require 'aws-sdk-s3'
require 'aws-sdk-s3/errors'
require_relative '../root'
require 'securerandom'
require 'fileutils'
require_relative '../invalid_key_error'
require_relative '../../error/md5'

class MedusaStorage::Root::S3 < MedusaStorage::Root

  attr_accessor :bucket, :region, :prefix, :aws_access_key_id, :aws_secret_access_key

  #md5_sum and mtime are rclone compatible names
  # in rclone the md5_sum is the base64 encoded 128 bit md5 sum
  # and mtime is seconds since the epoch, same as ruby Time.to_i
  AMAZON_HEADERS = {
      md5_sum: 'md5chksum',
      mtime: 'mtime'
  }

  def initialize(args = {})
    super(args)
    self.bucket = args[:bucket]
    self.region = args[:region]
    self.prefix = args[:prefix] || ''
    self.aws_access_key_id = args[:aws_access_key_id]
    self.aws_secret_access_key = args[:aws_secret_access_key]
  end

  def s3_client
    @s3_client ||= Aws::S3::Client.new(region: region, credentials: s3_credentials)
  end

  def s3_credentials
    Aws::Credentials.new(aws_access_key_id, aws_secret_access_key)
  end

  def presigner
    @presigner ||= Aws::S3::Presigner.new(client: s3_client)
  end

  #Do a head_object request on the key. This is to support other methods, but may be useful on its own.
  def info(key)
    s3_client.head_object(bucket: bucket, key: key)
  end

  def metadata(key)
    info(key).metadata
  end

  def md5_sum(key)
    metadata = metadata(key)
    metadata[AMAZON_HEADERS[:md5_sum]] || super(key)
  end

  def mtime(key)
    metadata(key)[AMAZON_HEADERS[:mtime]]
  end

  def exist?(key)
    info(key)
    true
  rescue Aws::S3::Errors::NotFound
    false
  end

  def size(key)
    info(key).content_length
  end

  def with_input_io(key)
    object = s3_client.get_object(bucket: bucket, key: key)
    body = object.body
    yield body
  ensure
    body.close if body
  end

  def with_input_file(key, tmp_dir: nil)
    tmp_dir ||= Dir.tmpdir
    sub_dir = File.join(tmp_dir, SecureRandom.hex(10))
    FileUtils.mkdir_p(sub_dir)
    file_name = File.join(sub_dir, File.basename(key))
    with_input_io(key) do |io|
      IO.copy_stream(io, file_name)
    end
    yield file_name
  ensure
    FileUtils.rm_rf(sub_dir) if sub_dir and Dir.exist?(sub_dir)
  end

  def copy_io_to(key, input_io, md5_sum, metadata = {})
    metadata_headers = Hash.new
    args = {bucket: bucket, key: key, body: input_io, content_md5: md5_sum, metadata: metadata_headers}
    metadata_headers[AMAZON_HEADERS[:md5_sum]] = md5_sum
    metadata_headers[AMAZON_HEADERS[:mtime]] = metadata[:mtime].to_i.to_s if metadata[:mtime]
    s3_client.put_object(args)
  rescue Aws::S3::Errors::InvalidDigest
    raise MedusaStorage::Error::MD5
  end

  #Get a 'GET' url for this object that is presigned, so can be used to grant access temporarily without the auth info.
  def presigned_get_url(key, args = {})
    presigner.presigned_url(:get_object, {bucket: bucket, key: key, expires_in: 7.days.to_i}.merge(args))
  end

  def file_keys(directory_key)
    internal_subtree_keys(directory_key, delimiter: '/')
  end

  def subtree_keys(directory_key)
    internal_subtree_keys(directory_key)
  end

  def subdirectory_keys(directory_key)
    keys = Array.new
    continuation_token = nil
    loop do
      results = s3_client.list_objects_v2(bucket: bucket, prefix: ensure_directory_key(directory_key), continuation_token: continuation_token, delimiter: delimiter)
      keys += results.common_prefixes.collect(&:key)
      continuation_token = results.next_continuation_token
      break if continuation_token.nil?
    end
    return keys
  end

  #internal method to support getting 'file' type objects
  def internal_subtree_keys(directory_key, delimiter: nil)
    keys = Array.new
    continuation_token = nil
    loop do
      results = s3_client.list_objects_v2(bucket: bucket, prefix: ensure_directory_key(directory_key), continuation_token: continuation_token, delimiter: delimiter)
      keys += results.contents.collect(&:key).reject {|key| directory_key?(key)}
      continuation_token = results.next_continuation_token
      break if continuation_token.nil?
    end
    return keys
  end

  def directory_key?(key)
    key.end_with?('/')
  end

  def ensure_directory_key(key)
    directory_key?(key) ? key : key + '/'
  end

  def delete_content(key)
    s3_client.delete_object(bucket: bucket, key: key)
  end

end