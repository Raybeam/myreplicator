require "myreplicator/engine"
require "exporter"

module Myreplicator
  mattr_accessor(:app_root, 
                 :tmp_path, 
                 :mysql, 
                 :mysqldump, 
                 :configs, 
                 :auth_required,
                 :authenticated,
                 :login_redirect)
  
  class Engine < Rails::Engine

    # Setting up engine configurations after host Rails app starts

    initializer "myreplicator.configure_rails_initialization" do |app|
      Myreplicator.app_root = app.root #assigning app root as rails.root is not accessible

      # myreplicator yml file is required

      yml = YAML.load(File.read("#{Myreplicator.app_root}/config/myreplicator.yml"))

      Myreplicator.mysql = yml["myreplicator"]["mysql"] # mysql path 
      Myreplicator.mysqldump = yml["myreplicator"]["mysqldump"] # mysqldump path 

      # Authentication Check

      if yml["myreplicator"]["auth_required"].blank?
        Myreplicator.auth_required = false
      else
        Myreplicator.auth_required = yml["myreplicator"]["auth_required"]
        Myreplicator.authenticated = false
        Myreplicator.login_redirect = yml["myreplicator"]["login_redirect"]
      end        

      # Temp directory path
      if yml["myreplicator"]["tmp_path"].blank?
        Myreplicator.tmp_path = File.join(Myreplicator.app_root, "tmp", "myreplicator")
      else
        Myreplicator.tmp_path = yml["myreplicator"]["tmp_path"]
      end

      Myreplicator.configs = yml
    end

  end

  # BOB : Usually you'd make a Myreplicator::Error inherit from StandardError,
  # then make all of the other exceptions inherit from Myreplicator::Error
  # With the way you have it set up, I can't catch all Myreplicator errors without specifying
  # each individually.
  # Check out how ActiveRecord sets up its errors for an example
  module Exceptions
    class MissingArgs < StandardError; end
    class ExportError < StandardError; end
    class LoaderError < StandardError; end
    class ExportIgnored < StandardError; end
  end

end
