#TODO More magic numbers that should be configurable should be moved here
#
# tmpdir - the temporary directory to use by default. If not set with tmpdir= it
# uses the environment variable 'MEDUSA_STORAGE_TMPDIR' or 'TMPDIR' if set,
# and if not then Dir.tmpdir. Some methods may provide a way to control this
# when calling them, but this may not always be available, so you may want to set
# this if the normal tmpdir way be limited in some way, as on an S3 EC2.
require 'tmpdir'
module MedusaStorage::Config
  module_function

  def tmpdir
    @tmpdir ||= (ENV['MEDUSA_STORAGE_TMPDIR'] || ENV['TMPDIR'] || Dir.tmpdir)
  end

  def tmpdir=(value)
    @tmpdir = value
  end

end