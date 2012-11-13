require 'json'

module Myreplicator
  class ExportMetadata

    attr_accessor(:export_time, 
                  :export_finished_at, 
                  :table, 
                  :database, 
                  :state, 
                  :incremental_col, 
                  :export_id, 
                  :incremental_val,
                  :ssh)

    def initialize *args
      options = args.extract_options!
      if options[:metadata_path] 
        load options[:metadata_path] 
      else
        set_attributes options
      end
    end
    
    def self.record *args
      options = args.extract_options!
      options.reverse_merge!(:export_time => Time.now,
                             :state => "starting")
      begin
        metadata = ExportMetadata.new 
        metadata.set_attributes options

        yield metadata
      
      rescue Exception => e
        metadata.state = "#{e.message}\n#{e.backtrace}"
        raise e
      ensure
        metadata.export_finished_at = Time.now
        metadata.state = "finished" if metadata.state == "starting"
        puts "meta in ensure"      
        Kernel.p metadata.ssh
        metadata.store!
      end
    end

    def to_json
      obj = {
        :export_time => @export_time,
        :table => @table,
        :database => @database,
        :state => @state,
        :incremental_col => @incremental_col,
        :incremental_val => @incremental_val,
        :export_id => @export_id,
        :filepath => @filepath
      }

      return obj.to_json
    end

    def store!
      Kernel.p self.to_json
      cmd = "cat #{self.to_json} > #{@filepath}.json"
      puts cmd
      puts "meta in store!"      
      Kernel.p @ssh
      @ssh.exec!(cmd)
      # File.open("#{@filepath}.json", 'w') {|f| f.write(self.to_json)}
    end

    def load metadata_path
      json = File.open(options[:metadata_path], "rb").read
      hash = JSON.parse(json)
      Kernel.p hash
      Kernel.p json
      set_attributes hash
    end

    def set_attributes options
      @export_time = options[:export_time] if options[:export_time]
      @table = options[:table] if options[:table]
      @database = options[:database] if options[:database]
      @state = options[:state] if options[:state] 
      @incremental_col = options[:incremental_col] if options[:incremental_col]
      @incremental_val = options[:incremental_val] if options[:incremental_val]
      @export_id = options[:export_id] if options[:export_id]
      @filepath = options[:filepath] if options[:filepath]
      @ssh = nil
    end

  end
end
