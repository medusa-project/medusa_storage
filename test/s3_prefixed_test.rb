require_relative 'test_helper'
require 'net/http'
require 'digest'
require_relative 'time_helper'

class S3PrefixedTest < Minitest::Test
  include TimeHelper

  @@test_number = 0

  def setup
    @@test_number += 1
    @bucket = "prefixed-#{@@test_number}"
    @prefix = 'my/prefix/'
    @root = MedusaStorage::RootFactory.create_root(type: 's3', name: 's3', endpoint: S3ServerHelper.endpoint,
                                                   bucket: @bucket, region: S3ServerHelper.region,
                                                   aws_access_key_id: S3ServerHelper.access_key,
                                                   aws_secret_access_key: S3ServerHelper.secret_key,
                                                   force_path_style: true, prefix: @prefix)
    @unprefixed_root = MedusaStorage::RootFactory.create_root(type: 's3', name: 's3', endpoint: S3ServerHelper.endpoint,
                                                   bucket: @bucket, region: S3ServerHelper.region,
                                                   aws_access_key_id: S3ServerHelper.access_key,
                                                   aws_secret_access_key: S3ServerHelper.secret_key,
                                                   force_path_style: true)
    S3ServerHelper.setup_bucket_and_fixtures(@bucket, prefix: @prefix)
  end
  
  def teardown
  end

  def assert_exist?(key)
    assert @root.exist?(key)
    assert @unprefixed_root.exist?(@prefix + key)
  end

  def assert_not_exist?(key)
    refute @root.exist?(key)
    refute @unprefixed_root.exist?(@prefix + key)
  end

  ###
  # Tests for methods in the MedusaStorage::Root::S3 class, including overrides, with prefix
  def test_add_prefix
    assert_equal 'my/prefix/key', @root.add_prefix('key')
    assert_equal 'my/prefix/', @root.add_prefix('')
  end

  def test_remove_prefix
    assert_equal 'key', @root.remove_prefix('my/prefix/key')
    assert_equal '', @root.remove_prefix('my/prefix/')
  end

  def test_size
    assert_equal 4, @root.size('joe.txt')
  end

  def test_md5_sum
    assert_equal "yE9w0br5ZCB9dRm/IWAsJA==", @root.md5_sum('joe.txt')
  end

  def test_existence
    assert_exist?('joe.txt')
    assert_exist?('child/fred.txt')
    assert_not_exist?('jared.txt')
    assert_not_exist?('child/jared.txt')
    assert_not_exist?('jared/joe.txt')
  end

  def test_file_keys
    assert_equal ['joe.txt', 'pete.txt'],
                 @root.file_keys('').sort
    assert_equal ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt'],
                 @root.file_keys('child/grandchild-1').sort
    assert_equal ['child/grandchild-2/mel.txt'], @root.file_keys('child/grandchild-2')
  end

  def test_file_keys_non_existent_directory
    assert_equal [], @root.file_keys('no-child')
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
    assert_exist?('child/fred.txt')
    @root.delete_content('child/fred.txt')
    assert_not_exist?('child/fred.txt')
  end

  def test_move_content
    assert_exist?('child/fred.txt')
    assert_not_exist?('fred-move.txt')
    @root.move_content('child/fred.txt', 'fred-move.txt')
    assert_not_exist?('child/fred.txt')
    assert_exist?('fred-move.txt')
    assert_equal "fred\n", @root.as_string('fred-move.txt')
  end

  def test_presigned_get_url
    url = @root.presigned_get_url('joe.txt')
    response = Net::HTTP.get(URI(url))
    assert_equal "joe\n", response
  end

  def test_with_input_io
    @root.with_input_io('child/fred.txt') do |io|
      assert_equal "fred\n", io.read
    end
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
    assert_exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert time_equal(now, @root.mtime('new.txt'))
    assert_equal string, @root.as_string('new.txt')
  end
  
  # # TODO I've removed this for now - it hangs minio for some reason with:
  # # S3 client configured for "us-east-1" but the bucket "medusa-storage" is in "us-east-1";
  # # Please configure the proper region to avoid multiple unnecessary redirects and signing attempts
  # # def test_copy_io_to_bad_md5
  # #   refute @root.exist?('new.txt')
  # #   string = 'new content'
  # #   size = string.length
  # #   md5 = "somerandomstring"
  # #   now = Time.now
  # #   assert_raises(MedusaStorage::Error::MD5) do
  # #     @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), md5, size,
  # #                      mtime: now)
  # #   end
  # #   refute @root.exist?('new.txt')
  # # end
  #
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
    assert_exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert_equal string, @root.as_string('new.txt')
  end

  def test_copy_io_to_overwriting
    assert_exist?('joe.txt')
    string = 'new content'
    size = string.length
    md5 = Digest::MD5.base64digest(string)
    now = Time.now
    @root.copy_io_to('joe.txt', StringIO.new(string, 'rb'), md5, size,
                     mtime: now)
    assert_exist?('joe.txt')
    assert_equal string, @root.as_string('joe.txt')
  end

  # ###
  # # Tests for methods in the MedusaStorage::Root base class not overridden
  #
  #

  def test_delete_tree_on_directory_key
    keys = ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt']
    keys.each {|key| assert_exist?(key)}
    @root.delete_tree('child/grandchild-1/')
    keys.each {|key| assert_not_exist?(key)}
    assert_exist?('child/grandchild-2/mel.txt')
  end

  def test_delete_tree_on_content_key
    assert_exist?('joe.txt')
    @root.delete_tree('joe.txt')
    assert_not_exist?('joe.txt')
  end

  def test_delete_all_content
    @unprefixed_root.write_string_to('above_prefix.txt', 'some string')
    all_keys = @root.subtree_keys('')
    assert_operator 0, :<, all_keys.count
    @root.delete_all_content
    all_keys = @root.subtree_keys('')
    assert_equal 0, all_keys.count
    assert @unprefixed_root.exist?('above_prefix.txt')
  end

  def test_unprefixed_subtree_keys
    assert_equal ["fred.txt", "grandchild-1/dave.txt", "grandchild-1/jim.txt", "grandchild-2/mel.txt"],
                 @root.unprefixed_subtree_keys('child/').sort
    assert_equal ['dave.txt', 'jim.txt'],
                 @root.unprefixed_subtree_keys('child/grandchild-1/').sort
  end

  def test_as_string
    assert_equal "pete\n", @root.as_string('pete.txt')
    assert_equal "fred\n", @root.as_string('child/fred.txt')
  end

  def test_write_string_to
    now = Time.now
    @root.write_string_to('new.txt', 'new', mtime: now)
    assert_exist?('new.txt')
    assert_equal 'new', @root.as_string('new.txt')
    assert time_equal(now, @root.mtime('new.txt'))
  end

  def test_copy_content_to
    @root.copy_content_to('joe-copy.txt', @root, 'joe.txt')
    assert_exist?('joe-copy.txt')
    assert time_equal(@root.mtime('joe.txt'), @root.mtime('joe-copy.txt'))
    assert_equal @root.as_string('joe.txt'), @root.as_string('joe-copy.txt')
  end

  def test_copy_tree_to
    @root.copy_tree_to('child-copy/', @root, 'child/')
    %w(fred.txt grandchild-1/dave.txt
       grandchild-1/jim.txt grandchild-2/mel.txt).each do |key|
      old_key = File.join('child', key)
      new_key = File.join('child-copy', key)
      assert_exist?(new_key)
      assert_equal @root.as_string(old_key), @root.as_string(new_key)
    end
  end

end