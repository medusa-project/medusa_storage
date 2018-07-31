require_relative './root_factory'

class MedusaStorage::RootSet

  attr_accessor :root_set

  def initialize(root_config)
    self.root_set = Hash.new
    initialize_roots(root_config)
  end

  def initialize_roots(root_config_list)
    root_config_list.each do |root_config|
      root_set[root_config[:name]] = MedusaStorage::RootFactory.create_root(root_config)
    end
  end
  
  def at(root_name)
    root_set[root_name]
  end

  def all_roots
    self.root_set.values
  end

  def all_root_names
    self.root_set.keys
  end

end