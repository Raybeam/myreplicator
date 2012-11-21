require "thread"

module Myreplicator
  class Parallelizer

    attr_accessor :block, :params, :queue

    def initialize *args
      options = args.extract_options!
      @queue = Queue.new
      @proc = options[:block]
      @params = options[:params]
      @max_threads = options[:max_threads]
      @threads = Queue.new
    end
    
    def run
      for i in 0..@queue.size do
        @threads << Thread.new{Transporter.new.instance_exec(@params,&@proc)}
      end
    end

    def check_threads
      
    end

  end
end
