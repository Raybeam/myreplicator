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
      return false unless hostname == Socket.gethostname
      begin
        Process.kill('TERM', pid)
        self.state = "killed"
        self.save!
      rescue Errno::ESRCH
        puts "pid #{pid} does not exist!"
        mark_dead
      end
    end

    ##
    # Checks to see if the PID of the log is active or not
    ##
    def running?
      logs = Log.where(:file => file, 
                       :job_type => job_type, 
                       :state => "running",
                       :export_id => export_id,
                       :hostname => hostname)
  
      if logs.count > 0
        logs.each do |log|
          begin
            Process.getpgid(log.pid)
            puts "still running #{filepath}"
            return true
          rescue Errno::ESRCH
            log.mark_dead
          end
        end
      end
      
      return false
    end

    ##
    # Clear all logs marked running that are not running
    ##
    def self.clear_deads
      logs = Log.where(:state => "running")
  
      if logs.count > 0
        logs.each do |log|
          begin
            Process.getpgid(log.pid) if hostname == Socket.gethostname
          rescue Errno::ESRCH
            log.mark_dead
          end
        end
      end
      
    end
    
    def mark_dead
      self.state = "dead"
      self.save!
    end

  end
end
