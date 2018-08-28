#In this type of  MedusaStorage::Root the key is simply the key into the S3 storage bucket.
# Some additional methods relevant to S3 are provided.

require 'aws-sdk-s3'
require 'aws-sdk-s3/errors'
require_relative '../root'
require 'securerandom'
require 'fileutils'
require_relative '../invalid_key_error'
require_relative '../error/md5'
require_relative '../etag_calculator'
require 'parallel'

class MedusaStorage::Root::S3 < MedusaStorage::Root

  attr_accessor :endpoint, :bucket, :region, :prefix, :aws_access_key_id, :aws_secret_access_key,
                :force_path_style, :client_args

  #md5_sum and mtime are rclone compatible names
  # in rclone the md5_sum is the base64 encoded 128 bit md5 sum
  # and mtime is seconds since the epoch, same as ruby Time.to_i
  AMAZON_HEADERS = {
      md5_sum: 'md5chksum',
      mtime: 'mtime'
  }

  def initialize(args = {})
    super(args)
    self.endpoint = args[:endpoint]
    self.bucket = args[:bucket]
    self.region = args[:region]
    self.prefix = args[:prefix] || ''
    self.aws_access_key_id = args[:aws_access_key_id]
    self.aws_secret_access_key = args[:aws_secret_access_key]
    self.force_path_style = args[:force_path_style] || false
    initialize_client_args
  end

  def s3_client
    Aws::S3::Client.new(client_args)
  end

  def initialize_client_args
    args = {credentials: s3_credentials, force_path_style: force_path_style}
    args.merge!(endpoint: endpoint) if endpoint
    args.merge!(region: region) if region
    self.client_args = args
  end

  def s3_credentials
    Aws::Credentials.new(aws_access_key_id, aws_secret_access_key)
  end

  def s3_object(key)
    Aws::S3::Resource.new(client: s3_client).bucket(bucket).object(add_prefix(key))
  end

  def presigner
    Aws::S3::Presigner.new(client: s3_client)
  end

  def root_type
    :s3
  end

  def add_prefix_single(key)
    prefix + key
  end

  def remove_prefix_single(key)
    key.sub(/^#{prefix}/, '')
  end

  def add_prefix(key_or_keys)
    if prefix == ''
      key_or_keys
    else
      if key_or_keys.is_a?(String)
        add_prefix_single(key_or_keys)
      else
        key_or_keys.collect {|key| add_prefix_single(key)}
      end
    end
  end

  def remove_prefix(key_or_keys)
    if prefix == ''
      key_or_keys
    else
      if key_or_keys.is_a?(String)
        remove_prefix_single(key_or_keys)
      else
        key_or_keys.collect {|key| remove_prefix_single(key)}
      end
    end
  end

  #Do a head_object request on the key. This is to support other methods, but may be useful on its own.
  def info(key)
    s3_client.head_object(bucket: bucket, key: add_prefix(key))
  end

  def metadata(key)
    info(key).metadata
  end

  def md5_sum(key)
    metadata = metadata(key)
    metadata[AMAZON_HEADERS[:md5_sum]] || super(key)
  end

  def mtime(key)
    if mtime_string = metadata(key)[AMAZON_HEADERS[:mtime]]
      Time.at(mtime_string.to_f)
    else
      nil
    end
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
    object = s3_client.get_object(bucket: bucket, key: add_prefix(key))
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

  AMAZON_CUTOFF_SIZE = 5 * 1024 * 1024 * 1024

  #TODO - enable this to do direct S3 to S3 copies in some cases
  # Specifically, we need to do the following things:
  # - check for copy compatibility: this should always be possible from self to self
  #   For others, add a 'copy_targets:' array to the _source_ root, with values the
  #   names of possible _target_ roots.
  # - if not compatible, fall back to super
  # - if compatible, check to see if we need to do multipart or not
  # - test to see how metadata works
  # - the AWS object#copy_to method (on the source object) allows arbitrary size (with multipart set if necessary)
  #   copies. The object#copy_from or client#copy_to methods are also available, but may be more restricted for size
  #   (don't know about object#copy_from). 
  def copy_content_to(key, source_root, source_key, metadata = {})
    super
  end

  def copy_io_to(key, input_io, md5_sum, size, metadata = {})
    if size.nil? or size >= AMAZON_CUTOFF_SIZE
      copy_io_to_large(key, input_io, md5_sum, metadata)
    else
      copy_io_to_small(key, input_io, md5_sum, metadata)
    end
  end

  def copy_io_to_small(key, input_io, md5_sum, metadata = {})
    metadata_headers = Hash.new
    args = {bucket: bucket, key: add_prefix(key), body: input_io, metadata: metadata_headers}
    args.merge!(content_md5: md5_sum) if md5_sum
    metadata_headers[AMAZON_HEADERS[:md5_sum]] = md5_sum if md5_sum
    metadata_headers[AMAZON_HEADERS[:mtime]] = metadata[:mtime].to_f.to_s if metadata[:mtime]
    s3_client.put_object(args)
  rescue Aws::S3::Errors::InvalidDigest
    raise MedusaStorage::Error::MD5
  end

  AMAZON_PART_SIZE = 5 * 1024 * 1024
  UPLOAD_BUFFER_SIZE = 64 * 1024

  def copy_io_to_large(key, input_io, md5_sum, metadata = {})
    metadata_headers = Hash.new
    metadata_headers[AMAZON_HEADERS[:md5_sum]] = md5_sum if md5_sum
    metadata_headers[AMAZON_HEADERS[:mtime]] = metadata[:mtime].to_f.to_s if metadata[:mtime]
    object = s3_object(key)
    object_already_exists = object.exists?
    digester = MedusaStorage::EtagCalculator.new(AMAZON_PART_SIZE)
    buffer = ''
    result = object.upload_stream(metadata: metadata_headers, part_size: AMAZON_PART_SIZE) do |stream|
      while input_io.read(UPLOAD_BUFFER_SIZE, buffer)
        stream << buffer
        digester << buffer
      end
    end
    raise "Unknown error uploading #{key} to storage root #{name}" unless result
    unless object.etag == digester.etag
      #delete if the object didn't already exist and is there, i.e. if somehow it got uploaded
      # incorrectly. I don't think we should be able to get here, but just in case.
      # TODO - we might be able to make this more robust by checking versioning, using the
      # S3 mod time, etc. I.e. record before hand like we do existence and then only delete
      # if changed.
      object.delete if !object_already_exists and object.exists?
      raise MedusaStorage::Error::MD5
    end
  end

  #Get a 'GET' url for this object that is presigned, so can be used to grant access temporarily without the auth info.
  DEFAULT_EXPIRATION_TIME = 7 * 24 * 60 * 60 # 7 days
  def presigned_get_url(key, args = {})
    presigner.presigned_url(:get_object, {bucket: bucket, key: add_prefix(key), expires_in: DEFAULT_EXPIRATION_TIME}.merge(args))
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
      results = s3_client.list_objects_v2(bucket: bucket, prefix: add_prefix(ensure_directory_key(directory_key)), continuation_token: continuation_token, delimiter: '/')
      keys += results.common_prefixes.collect(&:prefix)
      continuation_token = results.next_continuation_token
      break if continuation_token.nil?
    end
    return remove_prefix(keys)
  end

  #internal method to support getting 'file' type objects
  def internal_subtree_keys(directory_key, delimiter: nil)
    keys = Array.new
    continuation_token = nil
    loop do
      results = s3_client.list_objects_v2(bucket: bucket, prefix: add_prefix(ensure_directory_key(directory_key)), continuation_token: continuation_token, delimiter: delimiter)
      keys += results.contents.collect(&:key).reject {|key| directory_key?(key)}
      continuation_token = results.next_continuation_token
      break if continuation_token.nil?
    end
    return remove_prefix(keys)
  end

  def directory_key?(key)
    key.end_with?('/') or key == ''
  end

  def ensure_directory_key(key)
    directory_key?(key) ? key : key + '/'
  end

  def delete_content(key)
    s3_client.delete_object(bucket: bucket, key: add_prefix(key))
  end

  def move_content(source_key, target_key)
    s3_object(source_key).move_to(bucket: bucket, key: add_prefix(target_key))
  end

end