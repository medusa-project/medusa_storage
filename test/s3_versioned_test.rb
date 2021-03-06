require_relative 'test_helper'
require_relative 's3_server_helper'
require 'aws-sdk-s3'

class S3VersionedTest < Minitest::Test

  #set up class variable to track run number
  @@test_number = 0

  def setup
    @@test_number += 1
    @bucket = "versioned-#{@@test_number}"
    @root = MedusaStorage::RootFactory.create_root(S3ServerHelper.root_args(@bucket, versioned: true))
    S3ServerHelper.setup_bucket_and_fixtures(@bucket, copy_fixtures: false, versioned: true)
  end

  def test_versions_count
    @root.write_string_to('file', 'old version')
    @root.write_string_to('file', 'new version')
    @root.write_string_to('file', 'newer version')
    @root.write_string_to('filestuff', 'unrelated file with same prefix')
    @root.write_string_to('fi', 'unrelated file with partial prefix')
    @root.write_string_to('file/file', 'key with same prefix as directory')
    assert_equal 3, @root.versions('file').length
    assert_equal 2, @root.versions('file', object_versions: :old).length
    assert_equal 1, @root.versions('file', object_versions: :latest).length
  end

  def test_versions_count_with_delete
    @root.write_string_to('file', 'old version')
    @root.delete_content('file')
    @root.write_string_to('file', 'newer version')
    assert_equal 3, @root.versions('file').length
    assert_equal 2, @root.versions('file', delete_marker_handling: :remove).length
    assert_equal 1, @root.versions('file', delete_marker_handling: :only).length
  end

  def test_versions_current_content
    @root.write_string_to('file', 'new version')
    @root.write_string_to('file', 'newer version')
    assert_equal 'newer version', @root.as_string('file')
  end

  def test_versions_results
    @root.write_string_to('file', 'content')
    @root.delete_content('file')
    versions = @root.versions('file')
    content_version = versions.detect {|version| !version[:is_delete_marker]}
    assert content_version
    refute content_version[:is_latest]
    delete_marker = versions.detect {|version| version[:is_delete_marker]}
    assert delete_marker
    assert delete_marker[:is_latest]
  end

  def test_delete_tree_versions
    @root.write_string_to('file/subdir/0', 'content')
    @root.write_string_to('file/1', 'content')
    @root.delete_content('file/1')
    @root.write_string_to('file/2', 'other content')
    @root.write_string_to('file_3', 'content')
    @root.write_string_to('file_4', 'content')
    @root.delete_content('file_4')
    assert_equal 7, @root.delimited_prefix_versions('', delimiter: nil).length
    #note this tests conversion into directory key
    @root.delete_tree_versions('file')
    remaining_versions = @root.delimited_prefix_versions('', delimiter: nil)
    assert_equal 3, remaining_versions.length
    assert_equal 1, remaining_versions.select {|v| v[:key] == 'file_3'}.length
    assert_equal 2, remaining_versions.select {|v| v[:key] == 'file_4'}.length
    assert_equal 1, remaining_versions.select {|v| v[:is_delete_marker] }.length
  end

  def test_delete_past_version
    @root.write_string_to('key', 'oldest')
    @root.write_string_to('key', 'old')
    @root.write_string_to('key', 'new')
    versions = @root.versions('key')
    assert_equal 3, versions.length
    version_to_delete = versions.detect {|version| !version[:is_latest]}
    @root.delete_version('key', version_to_delete[:version_id])
    assert_equal 2, @root.versions('key').length
    assert_equal 'new', @root.as_string('key')
  end

  def test_delete_current_version
    @root.write_string_to('key', 'oldest')
    @root.write_string_to('key', 'old')
    @root.write_string_to('key', 'new')
    version_to_delete = @root.versions('key', object_versions: :latest).first
    @root.delete_version('key', version_to_delete[:version_id])
    assert_equal 'old', @root.as_string('key')
  end

  def test_delete_delete_marker
    @root.write_string_to('key', 'old')
    @root.write_string_to('key', 'new')
    @root.delete_content('key')
    assert_equal 3, @root.versions('key').length
    delete_marker = @root.versions('key', delete_marker_handling: :only).first
    @root.delete_version('key', delete_marker[:version_id])
    assert_equal 2, @root.versions('key').length
    assert_equal 'new', @root.as_string('key')
  end

  def test_undelete_tree
    @root.write_string_to('delete/normal', 'content')
    @root.write_string_to('delete/old_delete_marker', 'old')
    @root.delete_content('delete/old_delete_marker')
    @root.write_string_to('delete/old_delete_marker', 'new')
    @root.write_string_to('unaffected', 'constant')
    @root.delete_content('unaffected')
    @root.delete_tree('delete/')
    assert_equal 0, @root.subtree_keys('delete/').count
    @root.undelete_tree('delete/')
    assert_equal 'content', @root.as_string('delete/normal')
    assert_equal 'new', @root.as_string('delete/old_delete_marker')
    assert_equal 3, @root.versions('delete/old_delete_marker').count
  end

  def test_version_methods_throw_exception_on_unversioned_root
    @root.versioned = false
    assert_raises MedusaStorage::Error::UnsupportedOperation do
      @root.versions('file')
    end
    assert_raises MedusaStorage::Error::UnsupportedOperation do
      @root.delete_tree_versions('file/')
    end
    assert_raises MedusaStorage::Error::UnsupportedOperation do
      @root.delete_version('key', 'version_id')
    end
    assert_raises MedusaStorage::Error::UnsupportedOperation do
      @root.undelete_tree('file/')
    end
  end

end