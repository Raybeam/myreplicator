require_dependency "myreplicator/application_controller"

module Myreplicator

  class HomeController < ApplicationController
    def index  
      respond_to do |format|
        format.html # index.html.erb
        format.json { render json: @exports }
      end
    end
    
  end
end
