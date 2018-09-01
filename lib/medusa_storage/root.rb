#This is an abstract class to represent a location from which content can be addressed by 'keys' that
# speak of a directory-style relative location from wherever the 'root' is (though in fact it might
# be more generalizable than that). Examples would be relative paths
# from some directory path in a filesystem, or amazon keys in a given bucket and possibly with a prefix (note that
# the trailing '/' must be specified).
# Anything that can fit into that paradigm could work, though we specifically have in mind filesystems and AWS S3.
#
# It is undefined what happens if a key either starts with '/' or contains consecutive '/' characters.
#
# We'll try to collect here the interface that any subclass should satisfy. Subclasses may have their own
# unique methods as well as appropriate. For example, S3 is able to generate presigned urls to content, or
# the filesystem can produce an actual path to a file.
require 'base64'
require 'hex_string'
require 'parallel'

class MedusaStorage::Root

  attr_accessor :name

  def initialize(args = {})
    self.name = args[:name]
  end

  #Return a symbol indicating the type of this root
  def root_type
    raise "subclass responsibility"
  end

  #Return the size of the content at key
  def size(key)
    raise "subclass responsibility"
  end

  #return the base64 encoded md5 sum (as used by AWS - note that internally to the collection registry we use the
  # hex digest)
  #Note that subclasses may want to reimplement - in some circumstances this might be a look up rather than
  # a calculation. Or on the filesystem we can use the ruby methods explicitly.
  def md5_sum(key)
    md5 = Digest::MD5.new
    buffer = ''
    with_input_io(key) do |io|
      while io.read(65536, buffer)
        md5 << buffer
      end
    end
    md5.base64digest
  end

  def hex_md5_sum(key)
    Base64.decode64(md5_sum(key)).to_hex_string(false)
  end

  def mtime(key)
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

  #Return all descendants of the key that represent files/content, but without the prefix, i.e.
  # relative to the given key
  def unprefixed_subtree_keys(key)
    prefixed_keys = subtree_keys(key)
    prefix = key.end_with?('/') ? key : key + '/'
    prefixed_keys.collect {|k| k.sub(/^#{prefix}/, '')}
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
  # enough, so EFS may be indicated). This is found by MedusaStorage::Config.tmpdir. It may be set in
  # code by MedusaStorage::Config.tmpdir=. It may also be set with an environment variable or by default
  # uses Dir.tmpdir. See MedusaStorage::Config for details.
  #
  # This is provided as an alternative to with_input_io when an operation is to be performed that cannot work
  # directly on a stream, e.g. processing with FITS.
  def with_input_file(key, tmp_dir: nil)
    raise "subclass responsibility"
  end

  #The content of the key as a string - clearly care should be used if it could be large
  def as_string(key)
    with_input_io(key) do |io|
      io.read
    end
  end

  #Write the given string to the given key
  def write_string_to(key, string, mtime: nil)
    mtime ||= Time.now
    copy_io_to(key, StringIO.new(string), Digest::MD5.base64digest(string), string.length, mtime: mtime)
  end

  #copy the given io to the key
  # Should raise MedusaStorage::Error::MD5 if there is an md5 problem.
  # If the MD5 is unknown then pass nil. This will result in checks that use
  # the md5 being skipped and no md5 being persisted.
  # The size argument is present because the implementation might have to
  # dispatch on it and it is not necessarily obtainable from the io,
  # but you should be able to pass nil, in which case the implementation should
  # just assume that it needs to invoke code for the largest thing it can handle.
  # Ideally metadata will have an mtime: key (expressed in standard ruby Time form)
  # which will be persisted naturally for the backend. Other keys will be persisted
  # as possible. In short, the behavior there is not completely well defined yet.
  def copy_io_to(key, input_io, md5_sum, size, metadata = {})
    raise "subclass responsibility"
  end

  #copy content from a key in one source root to a key in another
  # a generic implementation is provided, but subclassess may override, e.g.
  # S3 might look to see if the target and source are both on the same S3 system
  # and if so invoke a copy directly on that system.
  def copy_content_to(key, source_root, source_key, metadata = {})
    source_root.with_input_io(source_key) do |io|
      copy_io_to(key, io, source_root.md5_sum(source_key), source_root.size(source_key),
                 {mtime: source_root.mtime(source_key)}.merge(metadata))
    end
  end

  #copy an entire tree. We take each file in the source_key subtree and use its unprefixed path
  # at the rest of the path under the target key. This is intended for small and/or test trees,
  # and could use some robustification for more general use. E.g. use parallel, check for existing
  # content, handle interruption, etc. Also some root types may be able to do this more efficiently.
  def copy_tree_to(key, source_root, source_key)
    source_root.unprefixed_subtree_keys(source_key).each do |unprefixed_key|
      copy_content_to(File.join(key, unprefixed_key), source_root, File.join(source_key, unprefixed_key))
    end
  end

  #Remove the content at this key
  def delete_content(key)
    raise "subclass responsibility"
  end

  #Move content to a different location in the root
  # Note that this default is not atomic and individual implementations can probably do better
  def move_content(source_key, target_key)
    copy_content_to(target_key, self, source_key)
    delete_content(source_key)
  end

  #If a file key then delete its content; if a directory key then delete all content under it
  # Subclasses may want to override for efficiency, or to provide better behavior, e.g. a filesystem
  # root will probably want to delete the directories as well as the content
  def delete_tree(key)
    if directory_key?(key)
      Parallel.each(subtree_keys(key), in_threads: 10) do |content_key|
        delete_content(content_key)
      end
    else
      delete_content(key)
    end
  end

  #Remove all content in this root. You probably want to be careful with this - it exists mostly
  # to facilitate testing. Subclasses may want to implement more efficiently
  def delete_all_content
    Parallel.each(subtree_keys(''), in_threads: 10) do |key|
      delete_content(key)
    end
  end

end