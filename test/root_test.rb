require_relative 'test_helper'

class RootTest < Minitest::Test

  def setup
    @root = MedusaStorage::Root.new(name: 'root')
  end

  #make sure methods which are supposed to be abstract raise an
  # error
  def test_abstract_methods
    %i(size mtime file_keys subdirectory_keys subtree_keys directory_key?
       exist? with_input_io with_input_file delete_content).each do |method|
      assert_raises(RuntimeError) {@root.send(method, 'key')}
    end
    assert_raises(RuntimeError) {@root.root_type}
    assert_raises(RuntimeError) {@root.copy_io_to('key', StringIO.new('string'), 'md5_sum', 6)}
  end

end