require "myreplicator/engine"


module Myreplicator
  mattr_accessor :app_root, :tmp_path, :mysql, :mysqldump
  
  class Engine < Rails::Engine

    initializer "myreplicator.configure_rails_initialization" do |app|
      Myreplicator.app_root = app.root #assigning app root as rails.root is not accessible
      yml = YAML.load(File.read("#{Myreplicator.app_root}/config/myreplicator.yml"))

      Myreplicator.mysql = yml["myreplicator"]["mysql"]
      Myreplicator.mysqldump = yml["myreplicator"]["mysqldump"]
      
      if yml["myreplicator"]["tmp_path"].blank?
        Myreplicator.tmp_path = File.join(Myreplicator.app_root, "tmp", "myreplicator")
      else
        Myreplicator.tmp_path = yml["myreplicator"]["tmp_path"]
      end
    end

  end

  module Exceptions
    class MissingArgs < StandardError; end
    class ExportError < StandardError; end
  end

end
