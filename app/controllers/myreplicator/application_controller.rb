module Myreplicator
  class ApplicationController < ActionController::Base

    before_filter :print
    
    def print
      puts "BEFORE IN MY APP"
    end

  end
end
