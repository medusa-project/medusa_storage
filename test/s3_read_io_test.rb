require_relative 'test_helper'

class S3ReadIOTest < Minitest::Test

  @@test_number = 0

  def setup
    @@test_number += 1
    @bucket = "readio-#{@@test_number}"
    @root = MedusaStorage::RootFactory.create_root(S3ServerHelper.root_args(@bucket))
    S3ServerHelper.setup_bucket_and_fixtures(@bucket, copy_fixtures: false)
  end

  def test_create_and_size_read_io
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    assert_equal 7, io.size
  end

  def test_read
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    assert_equal 'content', io.read
  end

  def test_seek_end
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    io.seek(-2, IO::SEEK_END)
    assert_equal 'nt', io.read
  end

  def test_seek_set
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    io.seek(2, IO::SEEK_SET)
    assert_equal 'ntent', io.read
  end

  def test_seek_cur
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    io.read(2)
    io.seek(2, IO::SEEK_CUR)
    assert_equal 'en', io.read(2)
  end

  def test_seek_bad_whence
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    assert_raises RuntimeError do
      io.seek(4, 'bad whence')
    end
  end

  def test_seek_bad_position
    @root.write_string_to('test', 'content')
    io = MedusaStorage::S3::ReadIO.new(@root, 'test')
    [-10, 10].each do |offset|
      assert_raises RuntimeError do
        io.seek(offset, IO::SEEK_SET)
      end
      assert_equal 0, io.pos
    end
  end

end