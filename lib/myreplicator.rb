require "myreplicator/engine"


module Myreplicator
  mattr_accessor :app_root
  
  class Engine < Rails::Engine

    initializer "myreplicator.configure_rails_initialization" do |app|
      Myreplicator.app_root = app.root #assigning app root as rails.root is not accessible
      require "configuration"
    end

  end

  module Exceptions
    class MissingArgs < StandardError; end
  end

end
