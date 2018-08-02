require_relative 'test_helper'
require_relative 'minio_helper'
require 'net/http'
require 'digest'

class S3UnprefixedTest < Minitest::Test

  def setup
    MinioHelper.install_fixtures
    @root = MedusaStorage::RootFactory.create_root(type: 's3', name: 's3', endpoint: MinioHelper.endpoint,
                                                   bucket: MinioHelper.bucket, region: MinioHelper.region,
                                                   aws_access_key_id: MinioHelper.access_key,
                                                   aws_secret_access_key: MinioHelper.secret_key,
                                                   force_path_style: true)
  end

  def teardown
    MinioHelper.remove_fixtures
  end

  ###
  # Tests for methods in the MedusaStorage::Root::S3 class, including overrides

  def test_root_type
    assert_equal :s3, @root.root_type
    assert @root.is_a?(MedusaStorage::Root::S3)
  end

  def test_size
    assert_equal 4, @root.size('joe.txt')
  end

  def test_md5_sum
    assert_equal "yE9w0br5ZCB9dRm/IWAsJA==", @root.md5_sum('joe.txt')
  end

  def test_existence
    assert @root.exist?('joe.txt')
    assert @root.exist?('child/fred.txt')
    refute @root.exist?('jared.txt')
    refute @root.exist?('child/jared.txt')
    refute @root.exist?('jared/joe.txt')
  end

  def test_file_keys
    assert_equal ['joe.txt', 'pete.txt'],
                 @root.file_keys('').sort
    assert_equal ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt'],
                 @root.file_keys('child/grandchild-1').sort
    assert_equal ['child/grandchild-2/mel.txt'], @root.file_keys('child/grandchild-2')
  end

  def test_subdirectory_keys
    assert_equal ['child/'], @root.subdirectory_keys('')
    assert_equal ['child/grandchild-1/', 'child/grandchild-2/'],
                 @root.subdirectory_keys('child')
  end

  def test_subtree_keys
    assert_equal ["child/fred.txt", "child/grandchild-1/dave.txt", "child/grandchild-1/jim.txt", "child/grandchild-2/mel.txt"],
                 @root.subtree_keys('child').sort
    assert_equal ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt'],
                 @root.subtree_keys('child/grandchild-1').sort
  end

  def test_delete_content
    assert @root.exist?('child/fred.txt')
    @root.delete_content('child/fred.txt')
    refute @root.exist?('child/fred.txt')
  end

  def test_presigned_get_url
    url = @root.presigned_get_url('joe.txt')
    response = Net::HTTP.get(URI(url))
    assert_equal "joe\n", response
  end

  ###
  # Tests for methods in the MedusaStorage::Root base class not overridden

  def test_name
    assert_equal 's3', @root.name
  end

  def test_mtime
    #There is nothing here because I don't think that minio
    # handles this in a way that is compatible with what we do.
    # I just record it in case we think of something to do later.
  end

  def test_with_input_file
    @root.with_input_file('pete.txt') do |filename|
      assert File.exist?(filename)
      assert_equal "pete\n", File.read(filename)
    end
  end

  def test_copy_io_to_small
    refute @root.exist?('new.txt')
    string = 'new content'
    size = string.length
    md5 = Digest::MD5.base64digest(string)
    now = Time.now
    @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), md5, size,
                     mtime: now)
    assert @root.exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert_equal string, @root.as_string('new.txt')
  end

  #Note that this doesn't test the dispatching at 5 GB, it merely does a relative large test, exceeding the part size
  # of 5 MB, at least
  def test_copy_io_to_large
    size = MedusaStorage::Root::S3::AMAZON_PART_SIZE * 16 #80MB for 5MB part size
    #Use the following to do a 10GB file
    #size = MedusaStorage::Root::S3::AMAZON_PART_SIZE * 2048
    refute @root.exist?('new.txt')
    read_io, write_io = IO.pipe
    writer = Thread.new do
      digest = Digest::MD5.new
      count = size / 1024
      string = "0123456789abcdef" * (1024 / 16)
      count.times do
        write_io.write(string)
        digest << string
      end
      writer[:md5] = digest.base64digest
      write_io.close
    end
    reader = Thread.new do
      @root.copy_io_to_large('new.txt', read_io, nil, mtime: Time.now)
      read_io.close
    end
    writer.join
    reader.join
    assert @root.exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal writer[:md5], @root.md5_sum('new.txt')
  end

  # TODO I've removed this for now - it hangs minio for some reason with:
  # S3 client configured for "us-east-1" but the bucket "medusa-storage" is in "us-east-1";
  # Please configure the proper region to avoid multiple unnecessary redirects and signing attempts
  # def test_copy_io_to_bad_md5
  #   refute @root.exist?('new.txt')
  #   string = 'new content'
  #   size = string.length
  #   md5 = "somerandomstring"
  #   now = Time.now
  #   assert_raises(MedusaStorage::Error::MD5) do
  #     @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), md5, size,
  #                      mtime: now)
  #   end
  #   refute @root.exist?('new.txt')
  # end

  #note that not providing the size forces this to go through
  # the 'large' code. We may want a separate one for a
  # legitimately large upload, though. Or not, depending on the
  # time considerations.
  def test_copy_io_to_no_md5_no_size
    refute @root.exist?('new.txt')
    string = 'new content'
    size = string.length
    md5 = Digest::MD5.base64digest(string)
    @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), nil, nil)
    assert @root.exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert_equal string, @root.as_string('new.txt')
  end

  def test_copy_io_to_overwriting
    assert @root.exist?('joe.txt')
    string = 'new content'
    size = string.length
    md5 = Digest::MD5.base64digest(string)
    now = Time.now
    @root.copy_io_to('joe.txt', StringIO.new(string, 'rb'), md5, size,
                     mtime: now)
    assert @root.exist?('joe.txt')
    assert_equal string, @root.as_string('joe.txt')
  end

end