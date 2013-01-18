require "thread"

##
# Executes given Procs in parallel using mulltiple threads
# Execution are closed under the Transporter class, i.e. all 
# Transporter methods are accessible
# Worker threads are managed by another thread which parallizes the process
# even further using the max_threads option. 
##

module Myreplicator
  class Parallelizer

    attr_accessor :queue

    def initialize *args
      options = args.extract_options!
      @queue = Queue.new
      @threads = []
      @max_threads = options[:max_threads].nil? ? 10 : options[:max_threads]     
      @klass = options[:klass].constantize
    end

    ##
    # Runs while there are jobs in the queue
    # Waits for a second and checks for available threads
    # Exits when all jobs are allocated in threads
    ##
    def run
      @done = false
      @manager_running = false
      reaper = nil

      while @queue.size > 0
        if @threads.size <= @max_threads
          @threads << Thread.new(@queue.pop) do |proc|
            @klass.new.instance_exec(proc[:params], &proc[:block])
          end
        else
          unless @manager_running
            reaper = manage_threads 
            @manager_running = true
          end
          sleep 1
        end
      end   
      
      # Run manager if thread size never reached max
      reaper = manage_threads unless @manager_running

      # Waits until all threads are completed
      # Before exiting
      reaper.join
    end
    
    ##
    # Clears dead threads, 
    # frees thread pool for more jobs
    # Exits when no more threads are left
    ##
    def manage_threads
      Thread.new do 
        while(@threads.size > 0)
          done = []
          @threads.each do |t|
            done << t if t.stop?
          end
          done.each{|d| @threads.delete(d)} # Clear dead threads
          
          # If no more jobs are left, mark done

          if done?
            @done = true
          else
            sleep 2 # Wait for more threads to spawn
          end

        end
      end
    end

    ##
    # Returns true when all jobs are processed and
    # no thread is running
    ##
    def done?
      if @queue.size == 0 && @threads.size == 0
        return true
      end
      return false
    end

  end
end
