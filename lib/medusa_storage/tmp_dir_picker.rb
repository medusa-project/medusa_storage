#Initialize by passing an array of two element arrays. The first element of each is a size
# and the second a potential tmp path. The array is ordered in descending order of the first element and stored.
# When the pick method is called, it searches this for the first size that is exceeded or equaled and returns the
# corresponding path. If none are, then it returns Dir.tmpdir.
#
# So it looks something like (in practice you'd probably order it manually in a config, but you don't have to):
#
# [[1000, 'medium/path'], [250, 'small/path'], [30000, 'large/path']]
#
# For this >= 30000 would pick 'large/path', >=1000 but < 30000 'medium/path', >= 250 but < 1000 'small/path',
# and < 250 Dir.tmpdir.
#
# A common usage might be to choose between the system tmp dir or an auxiliary for large files. In this case the
# spec could be merely [[big_size, 'big/tmp/dir']].
#
# If that isn't clear, see the tests for this.
class MedusaStorage::TmpDirPicker

  attr_accessor :tmp_dir_specs

  def initialize(tmp_dir_array)
    self.tmp_dir_specs = tmp_dir_array.sort_by(&:first).reverse
  end

  def pick(size)
    path = self.tmp_dir_specs.detect {|spec| size >= spec.first}
    path ? path.last : Dir.tmpdir
  end

end