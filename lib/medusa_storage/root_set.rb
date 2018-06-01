class MedusaStorage::RootSet

  attr_accessor :root_set

  def initialize(root_config)
    self.root_set = Hash.new
    initialize_roots(root_config)
  end

  def initialize_roots(root_config)
    root_config.each do |root|
      root_class = case root[:type].to_s
                   when 'filesystem', ''
                     MedusaStorage::Root::Filesystem
                   when 's3'
                     MedusaStorage::Root::S3
                   else
                     raise "Unrecognized storage root type"
                   end
      root_set[root[:name]] = root_class.new(root)
    end
  end

  def at(root_name)
    root_set[root_name]
  end

end