module Myreplicator
  class Log < ActiveRecord::Base
    attr_accessible(:pid,
                    :job_type,
                    :name,
                    :file,
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
    # File's names are supposed to be unique
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

          log.state = "completed"
        rescue Exception => e
          log.state = "error"
          log.error = e.message
          log.backtrace =  e.backtrace

        ensure
          log.finished_at = Time.now
          log.thread_state = Thread.current.status
          log.save!
        end
      end

    end

    ##
    # Kills the job if running
    # Using PID
    ##
    def kill
      begin
        Process.kill('TERM', pid)
      rescue Errno::ESRCH
        puts "pid #{pid} does not exist!"
      end
    end

    def running?
      logs = Log.where(:file => file, :job_type => job_type, :state => "running")
  
      if logs.count > 0
        logs.each do |log|
          if File.exists? "/proc/#{log.pid.to_s}"
            puts "still running #{filepath}"
            return true
          else
            log.state = "error"
            log.save!
          end
        end
      end

      return false
    end

  end
end
