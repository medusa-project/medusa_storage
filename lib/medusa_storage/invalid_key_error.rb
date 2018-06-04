module MedusaStorage
  class InvalidKeyError < RuntimeError

    attr_accessor :root, :key

    def initialize(root, key)
      self.root = root
      self.key = key
    end

  end
end