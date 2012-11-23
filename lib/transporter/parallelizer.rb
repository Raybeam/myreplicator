require "thread"

module Myreplicator
  class Parallelizer

    attr_accessor :queue

    def initialize *args
      options = args.extract_options!
      @queue = Queue.new
      @threads = []
      @max_threads = options[:max_threads].nil? ? 10 : options[:max_threads]
    end

    def run
      while @queue.size > 0
        if @threads.size <= @max_threads
          @threads << Thread.new(@queue.pop) do |proc|
            Thread.current[:status] = 'running'
            Transporter.new.instance_exec(proc[:params], &proc[:block])
            Thread.current[:status] = 'done'
          end
        end
      end 
      
      # done = false
      Thread.new do 
        counter =0
        while(counter < 5)
          puts @threads.size
          @threads.each do |t|
            puts t[:status]
          end
          counter += 1
          sleep 2
        end
      end

      Kernel.p @threads
    end

    def check_threads
      Thread.new do 
        @threads.each do |t|
          
        end
      end
    end

  end
end
