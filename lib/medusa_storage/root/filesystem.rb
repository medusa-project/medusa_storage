#In this type of MedusaStorage::Root the key is the relative path from the filesystem location of the storage root. An empty
# key '' represents the root itself.
# In addition to the common methods some additional file system specific methods will be provided.
require 'pathname'
class MedusaStorage::Root::Filesystem < MedusaStorage::Root

  attr_accessor :path, :pathname, :real_path

  def initialize(args = {})
    super(args)
    self.path = args[:path]
    self.pathname = Pathname.new(self.path)
    self.real_path = self.pathname.realpath.to_s
  end

  #We check to make sure that the content requested really lies beneath this root;
  #however, we return the path for linking using the given path so that it remains
  #useable if any symlinks along self.pathname are changed.
  def path_to(key)
    Pathname.new(File.join(self.pathname.to_s, key)).tap do |file_pathname|
      puts file_pathname.to_s
      absolute_path = file_pathname.realpath.to_s
      raise InvalidFileError.new(name, key) unless absolute_path.match(/^#{self.real_path}\//)
    end
  rescue Errno::ENOENT
    raise InvalidFileError.new(name, key)
  end

  def size(key)
    path_to(key).size
  end

  def relative_path_from(full_key, prefix_key)
    path_to(full_key).relative_path_from(path_to(prefix_key)).to_s
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

  protected

  def when_directory(key, raise_error_if_not_directory: true)
    if directory_key?(key)
      yield
    else
      raise "Provided key is not a directory" if raise_error_if_not_directory
    end
  end

end