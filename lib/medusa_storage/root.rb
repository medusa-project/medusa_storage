class MedusaStorage::Root

  attr_accessor :name

  def initialize(args = {})
    self.name = args[:name]
  end

end