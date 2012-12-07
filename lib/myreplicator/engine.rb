module Myreplicator
  class Engine < ::Rails::Engine
  
    isolate_namespace Myreplicator

    config.after_initialize do
      require "myreplicator/application_controller"
    end

  end
end

