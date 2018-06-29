class MedusaStorage::Error::InvalidDirectory < RuntimeError

  attr_accessor :key

  def initialize(key)
    self.key = key
  end

end