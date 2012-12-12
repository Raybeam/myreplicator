module Myreplicator
  class Log < ActiveRecord::Base
    attr_accessible(:pid,
                    :job_type,
                    :name,
                    :filepath,
                    :state,
                    :hostname,
                    :export_id,
                    :error,
                    :backtrace,
                    :guid,
                    :started_at,
                    :finished_at)
    
    
    def self.run *args
      options = args.extract_options!
      options.reverse_merge!(:started_at => Time.now,
                             :pid => Process.pid,
                             :hostname => Socket.gethostname,
                             :guid => SecureRandom.hex(5))

      log = Log.create options

      begin
        log.save!

        yield log

        log.state = "completed"
      rescue Exception
        log.state = "failed"
        
      ensure
        
        log.save!
      end
    end

    def running?
      Log.where(:filepath => filepath, :job_type => job_type, :state => "running")
    end

  end
end
