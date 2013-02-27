module VerticaDb
  class Base < ActiveRecord::Base
    establish_connection(ActiveRecord::Base.configurations["vertica"])
  end
end

Dir['vertica_db/*.rb'].each { | f | require File.expand_path(f) }