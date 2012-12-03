module Myreplicator
  class Engine < ::Rails::Engine
    # isolate_namespace Myreplicator
    metaclass = class << self; self; end
    class << self
        define_method(:isolate_namespace) do |mod|
      puts "NOT Defined"
      engine_name(ActiveSupport::Inflector.underscore(mod).gsub("/", "_"))
      
      self.routes.default_scope = { :module => ActiveSupport::Inflector.underscore(mod.name) }
      self.isolated = true
      
      unless mod.respond_to?(:railtie_namespace)
        name, railtie = engine_name, self
        
        mod.singleton_class.instance_eval do
          define_method(:railtie_namespace) { railtie }
          
          unless mod.respond_to?(:table_name_prefix)
            define_method(:table_name_prefix) { "#{name}_" }
          end
          
          unless mod.respond_to?(:use_relative_model_naming?)
            class_eval "def use_relative_model_naming?; true; end", __FILE__, __LINE__
          end
          
          unless mod.respond_to?(:railtie_helpers_paths)
            define_method(:railtie_helpers_paths) { railtie.helpers_paths }
          end
          
          unless mod.respond_to?(:railtie_routes_url_helpers)
            define_method(:railtie_routes_url_helpers) { railtie.routes_url_helpers }
          end
        end
      end
    end unless method_defined? :isolate_namespace
  end    
    # if Rails::Engine.method_defined?(:isolate_namespace)
    #   puts "Defined"
    #   isolate_namespace Myreplicator
    # else
      
    # end

    isolate_namespace Myreplicator
  end
end

