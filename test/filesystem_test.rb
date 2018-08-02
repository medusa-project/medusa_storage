require_relative 'test_helper'
require 'fileutils'
require 'pathname'
require 'digest'

class FilesystemTest < Minitest::Test

  def setup
    @test_dir = File.join(File.dirname(__FILE__), 'test-content')
    FileUtils.cp_r(File.join(File.dirname(__FILE__), 'fixtures'), @test_dir)
    @root = MedusaStorage::RootFactory.create_root(type: 'filesystem', name: 'test', path: @test_dir)
  end

  def teardown
    FileUtils.rm_rf(@test_dir)
  end

  ###
  # Tests for methods in the MedusaStorage::Root::Filesystem class, including overrides

  def test_root_type
    assert_equal :filesystem, @root.root_type
    assert @root.is_a?(MedusaStorage::Root::Filesystem)
  end

  def test_path_construction_without_checks
    assert_equal Pathname.new(File.join(@test_dir, 'key')), @root.path_to('key')
    assert_equal Pathname.new(File.join(@test_dir, 'path/with/segments')), @root.path_to('path/with/segments')
    assert_equal Pathname.new(@test_dir), @root.path_to(nil)
    assert_equal Pathname.new(@test_dir), @root.path_to('')
  end

  def test_path_construction_with_checks
    FileUtils.touch(File.join(@test_dir, 'present_key'))
    assert_equal Pathname.new(File.join(@test_dir, 'present_key')), @root.path_to('present_key', check_path: true)
    ['absent_key', '..'].each do |key|
      exception = assert_raises(MedusaStorage::InvalidKeyError) do
        @root.path_to(key, check_path: true)
      end
      assert_equal key, exception.key
      assert_equal @root, exception.root
    end
  end

  def test_relative_path_extraction
    assert_equal '', @root.relative_path_from('joe', 'joe')
    assert_equal 'child/grandchild', @root.relative_path_from('main/child/grandchild', 'main')
    assert_equal 'child/grandchild', @root.relative_path_from('main/sub/child/grandchild', 'main/sub')
  end

  def test_existence
    assert @root.exist?('joe.txt')
    assert @root.exist?('child/fred.txt')
    refute @root.exist?('child/joe.txt')
    refute @root.exist?('fred.txt')
  end

  def test_size
    assert_equal 4, @root.size('joe.txt')
  end

  def test_md5_sum
    assert_equal "yE9w0br5ZCB9dRm/IWAsJA==", @root.md5_sum('joe.txt')
  end

  def test_mtime
    now = Time.now
    FileUtils.touch(File.join(@test_dir, 'joe.txt'), mtime: now)
    assert_equal now, @root.mtime('joe.txt')
  end

  def test_file_keys
    assert_equal ['joe.txt', 'pete.txt'],
                 @root.file_keys('').sort
    assert_equal ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt'],
                 @root.file_keys('child/grandchild-1').sort
    assert_equal ['child/grandchild-2/mel.txt'], @root.file_keys('child/grandchild-2')
  end

  def test_file_keys_bad_directory
    exception = assert_raises(MedusaStorage::Error::InvalidDirectory) do
      @root.file_keys('joe.txt')
    end
    assert_equal 'joe.txt', exception.key
  end

  def test_subdirectory_keys
    assert_equal ['child'], @root.subdirectory_keys('')
    assert_equal ['child/grandchild-1', 'child/grandchild-2'],
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

  def test_delete_tree
    keys = ['child/grandchild-1/dave.txt', 'child/grandchild-1/jim.txt']
    keys.each {|key| assert @root.exist?(key)}
    @root.delete_tree('child/grandchild-1')
    keys.each {|key| refute @root.exist?(key)}
    assert @root.exist?('child/grandchild-2/mel.txt')
  end

  def test_delete_all_content
    all_keys = @root.subtree_keys('')
    assert_operator 0, :<, all_keys.count
    @root.delete_all_content
    all_keys = @root.subtree_keys('')
    assert_equal 0, all_keys.count
  end

  def test_with_input_io
    @root.with_input_io('child/fred.txt') do |io|
      assert io.binmode?
      assert_equal "fred\n", io.read
    end
  end

  def test_with_input_file
    @root.with_input_file('pete.txt') do |filename|
      assert File.exist?(filename)
      assert_equal "pete\n", File.read(filename)
    end
  end

  def test_with_output_io
    refute @root.exist?('grace.txt')
    @root.with_output_io('grace.txt') do |io|
      io.puts 'grace'
    end
    assert @root.exist?('grace.txt')
    assert_equal "grace\n", @root.with_input_io('grace.txt') {|io| io.read}
  end

  def test_copy_io_to
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
    assert_equal now, @root.mtime('new.txt')
    assert_equal string, File.read(@root.path_to('new.txt'))
    assert_equal '0640', File.stat(@root.path_to('new.txt')).mode.to_s(8).chars.last(4).join
  end

  def test_copy_io_to_bad_md5
    refute @root.exist?('new.txt')
    string = 'new content'
    size = string.length
    md5 = "somerandomstring"
    now = Time.now
    assert_raises(MedusaStorage::Error::MD5) do
      @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), md5, size,
                       mtime: now)
    end
    refute @root.exist?('new.txt')
  end

  def test_copy_io_to_no_md5_no_size
    refute @root.exist?('new.txt')
    string = 'new content'
    size = string.length
    md5 = Digest::MD5.base64digest(string)
    now = Time.now
    @root.copy_io_to('new.txt', StringIO.new(string, 'rb'), nil, nil)
    assert @root.exist?('new.txt')
    assert_equal size, @root.size('new.txt')
    assert_equal md5, @root.md5_sum('new.txt')
    assert_operator now, :<, @root.mtime('new.txt')
    assert_equal string, File.read(@root.path_to('new.txt'))
    assert_equal '0640', File.stat(@root.path_to('new.txt')).mode.to_s(8).chars.last(4).join
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
    assert_equal string, File.read(@root.path_to('joe.txt'))
  end

  ###
  # Tests for methods in the MedusaStorage::Root base class not overriden
  def test_name
    assert_equal 'test', @root.name
  end

  def test_hex_md5_sum
    assert_equal 'c84f70d1baf964207d7519bf21602c24', @root.hex_md5_sum('joe.txt')
  end

  def test_unprefixed_subtree_keys
    assert_equal ["fred.txt", "grandchild-1/dave.txt", "grandchild-1/jim.txt", "grandchild-2/mel.txt"],
                 @root.unprefixed_subtree_keys('child').sort
    assert_equal ['dave.txt', 'jim.txt'],
                 @root.unprefixed_subtree_keys('child/grandchild-1').sort
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
    assert_equal now, @root.mtime('new.txt')
  end

  def test_copy_content_to
    @root.copy_content_to('joe-copy.txt', @root, 'joe.txt')
    assert @root.exist?('joe-copy.txt')
    assert_equal @root.mtime('joe.txt').to_s, @root.mtime('joe-copy.txt').to_s
    assert_equal @root.as_string('joe.txt'), @root.as_string('joe-copy.txt')
  end

  def test_copy_tree_to
    @root.copy_tree_to('child-copy', @root, 'child')
    %w(fred.txt grandchild-1/dave.txt
       grandchild-1/jim.txt grandchild-2/mel.txt).each do |key|
      old_key = File.join('child', key)
      new_key = File.join('child-copy', key)
      assert @root.exist?(new_key)
      assert_equal @root.as_string(old_key), @root.as_string(new_key)
    end
  end

end
