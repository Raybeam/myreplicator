require_dependency "myreplicator/application_controller"

module Myreplicator

  class HomeController < ApplicationController
    def index  
      @tab = 'home'
      @option = 'overview'
      @exports = Export.order('state DESC')
      @logs = Log.where(:state => 'running').order("id DESC")
      @now = Time.zone.now
      respond_to do |format|
        format.html # index.html.erb
        format.json { render json: @exports }
      end
    end

    def export_errors
      @tab = 'home'
      @option = 'errors'
      @exports = Export.where("error is not null").order('source_schema ASC')    
      @logs = Log.where("state = 'error' AND job_type = 'export'").order("id DESC").limit(200)
      @count = Log.where("state = 'error' AND job_type = 'export'").count
    end
    
    def transport_errors
      @tab = 'home'
      @option = 'errors'
      @logs = Log.where("state = 'error' AND job_type = 'transporter'").order("id DESC").limit(200)
      @count = Log.where("state = 'error' AND job_type = 'transporter'").count
    end

    def load_errors
      @tab = 'home'
      @option = 'errors'
      @logs = Log.where("state = 'error' AND job_type = 'loader'").order("id DESC").limit(200)
      @count = Log.where("state = 'error' AND job_type = 'loader'").count
    end
    

    def kill
      @log = Log.find(params[:id])
      @log.kill
      redirect_to :action => 'index' 
    end
    
    def pause
      flash[:notice] = "Pause All DR Jobs"
      require 'rake'
      Rake::Task.load(Rails.root.to_s + "/lib/tasks/" + "maintenance.rake")
      resque_reload = Rake::Task['maintenance:stop_dr_jobs']
      resque_reload.reenable
      resque_reload.execute(ENV["RAILS_ENV"])
      @redis = Redis.new(:host => Settings[:redis][:host], :port => Settings[:redis][:port])
      @redis.set "under_maintenance", "true"
      redirect_to :action => 'index'
    end
        
    def resume
      flash[:notice] = "Resume All DR Jobs"
      require 'rake'
      Rake::Task.load(Rails.root.to_s + "/lib/tasks/" + "maintenance.rake")
      resque_reload = Rake::Task['maintenance:start_dr_jobs']
      resque_reload.reenable
      resque_reload.execute(ENV["RAILS_ENV"])
      @redis = Redis.new(:host => Settings[:redis][:host], :port => Settings[:redis][:port])
      @redis.set "under_maintenance", "false"
      redirect_to :action => 'index'
    end
  end
end
