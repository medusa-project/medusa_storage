require_relative 'test_helper'
require_relative 'minio_helper'
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
  end

  ###
  # Tests for methods in the MedusaStorage::Root base class not overridden

  def test_name
    assert_equal 's3', @root.name
  end

  def test_exist
    assert @root.exist?('joe.txt')
    assert "joe\n", @root.as_string('joe.txt')
  end

end