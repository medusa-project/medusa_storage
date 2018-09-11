require_relative 'test_helper'

class TmpDirPickerTest < Minitest::Test

  def setup
    @tmp_dir_picker = MedusaStorage::TmpDirPicker.new([[1000, 'medium/path'],
                                                       [250, 'small/path'],
                                                       [30000, 'large/path']])
  end

  def test_pick_directory
    assert_equal 'large/path', @tmp_dir_picker.pick(40000)
    assert_equal 'large/path', @tmp_dir_picker.pick(30000)
    assert_equal 'medium/path', @tmp_dir_picker.pick(29999)
    assert_equal 'medium/path', @tmp_dir_picker.pick(1001)
    assert_equal 'medium/path', @tmp_dir_picker.pick(1000)
    assert_equal 'small/path', @tmp_dir_picker.pick(999)
    assert_equal 'small/path', @tmp_dir_picker.pick(251)
    assert_equal 'small/path', @tmp_dir_picker.pick(250)
    assert_equal Dir.tmpdir, @tmp_dir_picker.pick(249)
    assert_equal Dir.tmpdir, @tmp_dir_picker.pick(1)
  end

end