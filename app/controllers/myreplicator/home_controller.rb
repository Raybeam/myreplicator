require_dependency "myreplicator/application_controller"

module Myreplicator

  class HomeController < ApplicationController
    def index  
      @tab = 'home'
      @option = 'overview'
      @exports = Export.order('state DESC')
      @logs = Log.where(:state => 'running').order("started_at DESC")
      @now = Time.zone.now
      respond_to do |format|
        format.html # index.html.erb
        format.json { render json: @exports }
      end
    end

    def errors
      @tab = 'home'
      @option = 'errors'
      @exports = Export.where("error is not null").order('source_schema ASC')    
      @logs = Log.where(:state => 'error').order("started_at DESC") 
    end
    
  end
end
