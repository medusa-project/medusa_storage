#This is an abstract class to represent a location from which content can be addressed by 'keys' that
# speak of a directory-style relative location from wherever the 'root' is (though in fact it might
# be more generalizable than that). Examples would be relative paths
# from some directory path in a filesystem, or amazon keys in a given bucket and possibly with a prefix. Anything
# that can fit into that paradigm could work, though we specifically have in mind filesystems and AWS S3.
#
# We'll try to collect here the interface that any subclass should satisfy. Subclasses may have their own
# unique methods as well as appropriate. For example, S3 is able to generate presigned urls to content, or
# the filesystem can produce an actual path to a file.
class MedusaStorage::Root

  attr_accessor :name

  def initialize(args = {})
    self.name = args[:name]
  end

  #Return the size of the content at key
  def size(key)
    raise "subclass responsibility"
  end

  #Return the immediate descendants of the key that represent files/content
  def file_keys(key)
    raise "subclass responsibility"
  end

  #Return the immediate descendants of the key that represent directories
  def subdirectory_keys(key)
    raise "subclass responsibility"
  end

  #Return all descendants of the key that represent files/content
  def subtree_keys(key)
    raise "subclass responsibility"
  end

  #Answer if key represents a directory
  def directory_key?(key)
    raise "subclass responsibility"
  end

  #Answer if key exists
  def exist?(key)
    raise "subclass responsibility"
  end

  #Yield the content stored at key to a block as a readable IO, which will automatically be closed.
  def with_input_io(key)
    raise "subclass responsibility"
  end

  #Yield a file path that will contain the content at key, creating a temporary copy there
  # if needed (note that you may get the actual content file, so be cautious - the expectation is
  # that the client will only read the file). If a temporary file is made then it will be removed
  # automatically when the block finishes.
  #
  # The tmp_dir parameter is provided so that the client has control over where in the
  # directory tree if needed (e.g. on EC2 systems the main storage of the server may not be large
  # enough, so EFS may be indicated).
  #
  # This is provided as an alternative to with_input_io when an operation is to be performed that cannot work
  # directly on a stream, e.g. processing with FITS.
  def with_input_file(key, tmp_dir: nil)
    raise "subclass responsibility"
  end

  #copy the given io to the key
  def copy_io_to(key, input_io)
    raise "subclass responsibility"
  end

  #Remove the content at this key
  def delete_content(key)
    raise "subclass responsibility"
  end

  #Remove all content in this root. You probably want to be careful with this - it exists mostly
  # to facilitate testing. Subclasses may want to implement more efficiently
  def delete_all_content
    Parallel.each(subtree_keys('')) do |key|
      delete_content(key)
    end
  end

end