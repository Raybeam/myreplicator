module Myreplicator
  class Log < ActiveRecord::Base
    attr_accessible(:pid,
                    :job_type,
                    :name,
                    :filepath,
                    :state,
                    :thread_state,
                    :hostname,
                    :export_id,
                    :error,
                    :backtrace,
                    :guid,
                    :started_at,
                    :finished_at)
    
    ##
    # Creates a log object
    # Stores the state and related information about the job
    ##
    def self.run *args
      options = args.extract_options!
      options.reverse_merge!(:started_at => Time.now,
                             :pid => Process.pid,
                             :hostname => Socket.gethostname,
                             :guid => SecureRandom.hex(5),
                             :thread_state => Thread.current.status,
                             :state => "new")

      log = Log.create options

      unless log.running?
        begin
          log.state = "running"
          log.save!

          yield log
          the export process works 
          log.state = "completed"

        rescue Exception => e
          log.state = "failed"
          log.error = e.message
          log.backtrace =  e.backtrace

        ensure
          log.save!
        end
      end

    end

    def running?
      Log.where(:filepath => filepath, :job_type => job_type, :state => "running").count > 0
    end

  end
end
