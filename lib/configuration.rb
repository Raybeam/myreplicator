require "myreplicator"

module Myreplicator
  ##
  # Configuration class for avoiding load the yml file everytime and cleaner config settings
  ##
  class Configuration
    class << self; attr_accessor :tmp_path, :mysqldump, :mysql end
    
    yml = YAML.load(File.read("#{Myreplicator.app_root}/config/myreplicator.yml"))
    Kernel.p yml["myreplicator"]

    @@tmp_path = yml["myreplicator"]["tmp_path"]
    @@mysql = yml["myreplicator"]["mysql"]
    @@mysqldump = yml["myreplicator"]["mysqldump"]
  end

  def self.config(&block)
    @@config ||= Myreplicator::Configuration.new

    yield @@config if block

    return @@config
  end
end
