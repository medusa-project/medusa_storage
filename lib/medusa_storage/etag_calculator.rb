#For calculating the etag of a multipart upload to S3
# This is found by computing the binary md5 of each part, concatenating,
# taking the MD5 hex digest of the string, appending '-' and the part count.
# We are assuming here (as in our usage) that each part except the last
# has the same size. Essentially, this is made to fit with the
# Aws::S3::Object.upload_stream method.
require 'base64'
require 'digest'
class MedusaStorage::EtagCalculator

  attr_accessor :part_size, :active_digest, :digests, :bytes_to_go, :total_bytes

  def initialize(part_size)
    self.part_size = part_size
    self.digests = Array.new
    self.active_digest = nil
    self.bytes_to_go = 0
    self.total_bytes = 0
  end

  #It's clearest to define this recursively
  def <<(string)
    content_length = string.bytesize
    if content_length.zero?
      #do nothing if no content is given
    elsif bytes_to_go.zero?
      #In this case we're done with the current digester (if present), so make a new active one
      # and recall this method. This will be where the process starts, given our initialization.
      self.bytes_to_go = part_size
      self.active_digest = Digest::MD5.new
      self.digests << active_digest
      self << string
    elsif content_length > bytes_to_go
      #put part of the string on the current digester and recall this method with the rest
      io = StringIO.new(string, 'rb')
      self.active_digest << io.read(bytes_to_go)
      self.total_bytes += bytes_to_go
      self.bytes_to_go = 0
      self << io.read
    else
      #put all of the string on the current digester and adjust bytes to go
      self.active_digest << string
      self.bytes_to_go -= content_length
      self.total_bytes += content_length
    end
  end

  def etag
    binary_digests = digests.collect do |d|
      Base64.decode64(d.base64digest)
    end
    %Q("#{Digest::MD5.hexdigest(binary_digests.join())}-#{digests.count}")
  end

end