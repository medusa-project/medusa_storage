require_relative 'root/s3'
require_relative 'root/filesystem'
module MedusaStorage::RootFactory

  module_function

  def root_class(root_config)
    case root_config[:type].to_s
    when 'filesystem', ''
      MedusaStorage::Root::Filesystem
    when 's3'
      MedusaStorage::Root::S3
    else
      raise "Unrecognized storage root type"
    end
  end

  def create_root(root_config)
    root_class(root_config).new(root_config)
  end

end