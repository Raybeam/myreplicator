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
                             :thread_state => Thread.current.to_s,
                             :state => "new")

      log = Log.create options

      if  !log.running?
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
          log.save!
        end
      else
        begin
          log.state = "ignored"
        rescue Exception => e
          log.state = "error"
          log.error = e.message
          log.backtrace =  e.backtrace
        ensure
          log.finished_at = Time.now
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
        Process.kill('KILL', pid)
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
            puts "still running #{log.file}"
            return true
          rescue Errno::ESRCH
            log.mark_dead
          end
        end
      end
      
      return false
    end
    
    ##
    # Clear all logs marked running that have run for more than 2 hours
    ##
    def self.clear_stucks
      runnings = Log.where(:state => "running")
      news = Log.where(:state => "new")
      logs = runnings + news
      
      if logs.count > 0
        logs.each do |log|
          
          time_start = log.started_at
          now = Time.now()
          if time_start + 2.hour < now
            begin
              Process.kill('KILL', log.pid)
              log.state = "killed"
              log.save!
            rescue Errno::ESRCH
              log.state = "dead"
              log.save!
            end  
          end
        end
      end
      
    end

    
    ##
    # Clear all logs marked running that are not running
    ##
    def self.clear_deads
      logs = Log.where(:state => "running")
  
      if logs.count > 0
        logs.each do |log|
          begin
            Process.getpgid(log.pid) if log.hostname == Socket.gethostname
          rescue Errno::ESRCH
            log.mark_dead
          end
        end
      end
      
    end

    ##
    # Gets a jobtype, file and export_id
    # returns true if the job is completed
    ##
    def self.completed? *args
      options = args.extract_options!
      log = Log.where(:export_id => options[:export_id],
                      :file => options[:file],
                      :job_type => options[:job_type]).last
      #Kernel.p "===== transport_complete? log ====="
      #Kernel.p log
      if log.nil?
        #return false
        # Bug: no log of tranporter job. Using return true for now
        return true
      else
        return true if log.state == "completed"
      end
      
      return false
    end

    def mark_dead
      self.state = "dead"
      self.save!
    end

  end
end
