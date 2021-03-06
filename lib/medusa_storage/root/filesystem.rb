#In this type of MedusaStorage::Root the key is the relative path from the filesystem location of the storage root. An empty
# key '' represents the root itself.
# In addition to the common methods some additional file system specific methods will be provided.
require 'pathname'
require 'fileutils'
require_relative '../root'
require_relative '../invalid_key_error'
require_relative '../error/md5'
require_relative '../error/invalid_directory'

class MedusaStorage::Root::Filesystem < MedusaStorage::Root

  attr_accessor :path, :pathname

  def initialize(args = {})
    super(args)
    self.path = args[:path]
    self.pathname = Pathname.new(self.path)
    begin
      initialize_real_path
    rescue Exception => e
      STDERR.puts "Could not find real path for storage root #{self.name}. Error: #{e}. "
    end
  end

  def real_path=(path)
    @real_path = path
  end

  def real_path
    @real_path || initialize_real_path
  end

  def initialize_real_path
    self.real_path = self.pathname.realpath.to_s
  end

  def root_type
    :filesystem
  end

  #Returns the file system path to the key, respecting symlinks and such, but also optionally does a check
  # to make sure that the target is actually under the root on the filesystem and throws an
  # error if it is not.
  def path_to(key, check_path: false)
    return self.pathname if key == '' or key.nil?
    #Note that we explicitly avoid pathname.join here - if the argument is absolute then something
    # unexpected happens, e.g. self.pathname.join('/') yields '/', not self.pathname + '/'.
    # File.join behaves more intuitively.
    Pathname.new(File.join(self.pathname.to_s, key)).tap do |file_pathname|
      if check_path
        absolute_path = file_pathname.realpath.to_s
        raise MedusaStorage::InvalidKeyError.new(self, key) unless absolute_path.match(/^#{self.real_path}\//)
      end
    end
  rescue Errno::ENOENT
    raise MedusaStorage::InvalidKeyError.new(self, key)
  end

  def size(key)
    path_to(key).size
  end

  def md5_sum(key)
    Digest::MD5.file(path_to(key).to_s).base64digest
  end

  def mtime(key)
    path_to(key).mtime
  end

  #gives a relative path to full_key from prefix_key
  def relative_path_from(full_key, prefix_key)
    relative_path = path_to(full_key).relative_path_from(path_to(prefix_key)).to_s
    relative_path == '.' ? '' : relative_path
  end

  def file_keys(key)
    when_directory(key) do
      path_to(key).children.select {|child| child.file?}.collect {|file| file.relative_path_from(self.pathname).to_s}
    end
  end

  def subtree_keys(key)
    when_directory(key) do
      files = Array.new
      directories = [key]
      while directory_key = directories.shift
        files += file_keys(directory_key)
        directories += subdirectory_keys(directory_key)
      end
      return files
    end
  end

  def subdirectory_keys(key)
    when_directory(key) do
      path_to(key).children.select {|child| child.directory?}.collect {|file| file.relative_path_from(self.pathname).to_s}
    end
  end

  def directory_key?(key)
    path_to(key).directory?
  end

  def exist?(key)
    path_to(key).exist?
  end

  def with_input_io(key)
    f = File.open(path_to(key), 'rb')
    yield f
  ensure
    f.close if f
  end

  def with_input_file(key, tmp_dir: nil)
    yield path_to(key).to_s
  end

  def with_output_io(key)
    target_pathname = path_to(key)
    target_pathname.dirname.mkpath
    f = File.open(target_pathname, 'wb')
    yield f
  ensure
    f.close if f
  end

  def copy_io_to(key, input_io, md5_sum, size, metadata = {})
    target_pathname = path_to(key)
    target_pathname.dirname.mkpath
    key_md5_sum = Digest::MD5.hexdigest(key)
    temp_pathname = target_pathname.dirname.join(key_md5_sum)
    File.open(temp_pathname.to_s, 'wb') do |temp_file|
      IO.copy_stream(input_io, temp_file)
    end
    if md5_sum and md5_sum != Digest::MD5.file(temp_pathname.to_s).base64digest
        temp_pathname.unlink if temp_pathname.file?
        raise MedusaStorage::Error::MD5
    end
    temp_pathname.chmod(0640)
    FileUtils.touch(temp_pathname.to_s, mtime: metadata[:mtime]) if metadata[:mtime]
    FileUtils.move(temp_pathname.to_s, target_pathname.to_s, force: true)
  ensure
    temp_pathname.unlink if temp_pathname and temp_pathname.file?
  end

  def delete_content(key)
    path_to(key).unlink
  end

  def move_content(source_key, target_key)
    FileUtils.mv(path_to(source_key), path_to(target_key), force: true)
  end

  #We override this to use the file system facilities to delete, which also ensures that directories will
  # get deleted, which the superclass method can't do.
  def delete_tree(key)
    FileUtils.rm_rf(path_to(key))
  end

  def delete_all_content
    Dir[File.join(path, '*')].each do |dir|
      FileUtils.rm_rf(dir)
    end
  end

  protected

  #Execute the block if the given key is a directory, optionally throwing an error if it is not (default is to do so)
  def when_directory(key, raise_error_if_not_directory: true)
    if directory_key?(key)
      yield
    else
      raise MedusaStorage::Error::InvalidDirectory.new(key) if raise_error_if_not_directory
    end
  end

end