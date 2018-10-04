#This exposes an S3 object as an IO using the io-like gem.
# This is useful in some circumstances - in particular, we can
# get an IO on an object without reading all of it into memory at
# once.
# However, it's still not a fit for everything. E.g. I tried it with
# the marcel/mimemagic gems, which seem to do a lot of rewinding. It
# doesn't work so well for that. It's still best for either reading
# a small piece of the file seekably, or reading through the entire
# file while limiting memory usage. Something that does a lot of seeking
# is probably not going to work that well.
require 'io/like'
require_relative '../s3'

class MedusaStorage::S3::ReadIO

  include IO::Like

  attr_accessor :root, :key, :position

  def initialize(root, key)
    self.root = root
    self.key = key
    self.position = 0
    self.fill_size = 10 * 1024 * 1024
  end

  def size
    @size ||= root.size(key)
  end

  MAX_READ_LENGTH = 10 * 1024 * 1024

  def unbuffered_read(length)
    raise EOFError if position >= size
    begin
      buffer = StringIO.new
      buffer.binmode
      bytes_read = 0
      while true
        read_length = [MAX_READ_LENGTH, length, size - (position + bytes_read)].min
        break if read_length.zero?
        new_bytes = unbuffered_single_read(position + bytes_read, read_length)
        IO.copy_stream(new_bytes, buffer)
        length = length - read_length
        bytes_read = bytes_read + read_length
      end
      self.position = self.position + bytes_read
      buffer.string
    rescue
      raise SystemCallError
    end
  end

  def unbuffered_single_read(start, length)
    root.get_bytes(key, start, length)
  end

  def unbuffered_seek(offset, whence = IO::SEEK_SET)
    case whence
    when IO::SEEK_SET, :SET
      self.position = offset
    when IO::SEEK_CUR, :CUR
      self.position = self.position + offset
    when IO::SEEK_END, :END
      self.position = size + offset
    else
      raise "Unrecognized seek whence: #{whence}"
    end
    raise "Seek out of range: position #{position} not in 0-#{size - 1}" unless position >= 0 and position <= size
  end

end