require_relative 'test_helper'
require 'net/http'
require 'digest'
require_relative 'time_helper'

class S3UnprefixedTest < Minitest::Test

  include TimeHelper

  @@test_number = 0

  def setup
    @@test_number += 1
    @bucket = "unprefixed-#{@@test_number}"
    @root = MedusaStorage::RootFactory.create_root(S3ServerHelper.root_args(@bucket))
    S3ServerHelper.setup_bucket_and_fixtures(@bucket)
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
    assert @root.exist?('child/fred.txt')
    @root.delete_content('child/fred.txt')
    refute @root.exist?('child/fred.txt')
  end

  def test_move_content
    assert @root.exist?('child/fred.txt')
    refute @root.exist?('fred-move.txt')
    @root.move_content('child/fred.txt', 'fred-move.txt')
    refute @root.exist?('child/fred.txt')
    assert @root.exist?('fred-move.txt')
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
    assert @root.exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert time_equal(now, @root.mtime('new.txt'))
    assert_equal string, @root.as_string('new.txt')
  end

  #Note that this doesn't test the dispatching at 5 GB, it merely does a relative large test, exceeding the part size
  # of 5 MB, at least
  #TODO - note that the underlying copy_io_to_large method doesn't always work properly with JRuby, hence we
  # avoid executing this test in that case
  unless RUBY_PLATFORM == 'java'
    def test_copy_io_to_large
      size = MedusaStorage::Root::S3::AMAZON_PART_SIZE * 16 #80MB for 5MB part size
      #Use the following to do a 10GB file
      #size = MedusaStorage::Root::S3::AMAZON_PART_SIZE * 2048
      now = Time.now
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
        @root.copy_io_to_large('new.txt', read_io, nil, mtime: now)
        read_io.close
      end
      writer.join
      reader.join
      assert @root.exist?('new.txt')
      assert_equal size, @root.size('new.txt')
      assert time_equal(now, @root.mtime('new.txt'))
      assert_equal writer[:md5], @root.md5_sum('new.txt')
    end
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
    assert time_equal(now, @root.mtime('joe.txt'))
    assert_equal string, @root.as_string('joe.txt')
  end

  ###
  # Tests for methods in the MedusaStorage::Root base class not overridden

  def test_name
    assert_equal 's3', @root.name
  end

  def test_mtime
    time = Time.at(Time.now.to_f - 36000)
    @root.write_string_to('mtime-test.txt', 'content', mtime: time)
    assert time_equal(time, @root.mtime('mtime-test.txt'))
  end

  def test_delete_tree_on_directory_key
    keys = ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt']
    keys.each {|key| assert @root.exist?(key)}
    @root.delete_tree('child/grandchild-1/')
    keys.each {|key| refute @root.exist?(key)}
    assert @root.exist?('child/grandchild-2/mel.txt')
  end

  def test_delete_tree_on_content_key
    assert @root.exist?('joe.txt')
    @root.delete_tree('joe.txt')
    refute @root.exist?('joe.txt')
  end

  def test_delete_all_content
    all_keys = @root.subtree_keys('')
    assert_operator 0, :<, all_keys.count
    @root.delete_all_content
    all_keys = @root.subtree_keys('')
    assert_equal 0, all_keys.count
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
    assert @root.exist?('new.txt')
    assert_equal 'new', @root.as_string('new.txt')
    assert time_equal(now, @root.mtime('new.txt'))
  end

  def test_copy_content_to
    @root.copy_content_to('joe-copy.txt', @root, 'joe.txt')
    assert @root.exist?('joe-copy.txt')
    assert time_equal(@root.mtime('joe.txt'), @root.mtime('joe-copy.txt'))
    assert_equal @root.as_string('joe.txt'), @root.as_string('joe-copy.txt')
  end

  def test_copy_content_to_force_super_call
    @root.copy_targets.delete(@root.name)
    refute @root.can_s3_copy_to?(@root.name)
    @root.copy_content_to('joe-copy.txt', @root, 'joe.txt')
    assert @root.exist?('joe-copy.txt')
    assert time_equal(@root.mtime('joe.txt'), @root.mtime('joe-copy.txt'))
    assert_equal @root.as_string('joe.txt'), @root.as_string('joe-copy.txt')
  end

  def test_copy_content_to_with_metadata
    now = Time.now
    @root.write_string_to('new.txt', 'new', mtime: now)
    @root.copy_content_to('new-copy.txt', @root, 'new.txt', 'some_key' => 'some_value')
    assert @root.exist?('new-copy.txt')
    assert_equal 'some_value', @root.metadata('new-copy.txt')['some_key']
    assert_equal @root.md5_sum('new.txt'), @root.md5_sum('new-copy.txt')
    assert time_equal(now, @root.mtime('new-copy.txt'))
  end

  def test_copy_tree_to
    @root.copy_tree_to('child-copy/', @root, 'child/')
    %w(fred.txt grandchild-1/dave.txt
       grandchild-1/jim.txt grandchild-2/mel.txt).each do |key|
      old_key = File.join('child', key)
      new_key = File.join('child-copy', key)
      assert @root.exist?(new_key)
      assert_equal @root.as_string(old_key), @root.as_string(new_key)
    end
  end

  def test_get_bytes
    @root.write_string_to('bytes.txt', "some random string")
    assert_equal 'random', @root.get_bytes('bytes.txt', 5, 6).string
  end

end