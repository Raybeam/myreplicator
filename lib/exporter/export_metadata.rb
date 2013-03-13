require 'json'

module Myreplicator
  class ExportMetadata

    attr_accessor(:export_time, 
                  :export_finished_at, 
                  :export_to,
                  :table, 
                  :database, 
                  :state, 
                  :incremental_col, 
                  :export_id, 
                  :incremental_val,
                  :ssh,
                  :export_type,
                  :store_in,
                  :on_duplicate,
                  :filepath,
                  :zipped,
                  :error)

    attr_reader :failure_callbacks
    attr_reader :success_callbacks
    attr_reader :ensure_callbacks
    attr_reader :ignore_callbacks

    def initialize *args
      options = args.extract_options!
      if options[:metadata_path] 
        load options[:metadata_path] 
      else
        set_attributes options
      end
    end
    
    def filename
      name = filepath.split("/").last
      name = zipped ? "#{name}.gz" : name
      return name     
    end
    
    def metadata_filename
      name = filepath.split("/").last
      name += ".json"
      return name
    end

    def metadata_filepath tmp_dir
      File.join(tmp_dir, metadata_filename)
    end

    def destination_filepath tmp_dir
      File.join(tmp_dir, filename)
    end

    ##
    # Compares the object with another metadata object
    # Return true if they are for the same table
    ## 
    def equals object
      if table == object.table && database == object.database
        return true
      end
      return false
    end

    ##
    # Keeps track of the state of the export
    # Stores itself in a JSON file on exit
    ##
    def self.record *args
      options = args.extract_options!
      options.reverse_merge!(:export_time => Time.now,
                             :state => "exporting")
      begin
        metadata = ExportMetadata.new 
        metadata.set_attributes options

        yield metadata

        metadata.run_success_callbacks

      rescue Exceptions::ExportError => e
        metadata.state = "failed"     
        metadata.error =  "#{e.message}\n#{e.backtrace}"
        metadata.run_failure_callbacks

      rescue Exceptions::ExportIgnored => e
        metadata.state = "ignored"
        metadata.run_ignore_callbacks
        metadata.filepath = metadata.filepath + ".ignored"

      ensure
        metadata.export_finished_at = Time.now
        metadata.state = "failed" if metadata.state == "exporting"
        metadata.store!
        metadata.ssh.close

        metadata.run_ensure_callbacks
      end
    end

    # Add a callback to run on failure of the 
    # export
    def on_failure *args, &block
      if block_given?
        @failure_callbacks << block
      else
        @failure_callbacks << args.shift
      end
    end

    # Adds a callback that runs if the
    # export is already running
    def on_ignore *args, &block
      if block_given?
        @ignore_callbacks << block
      else
        @ignore_callbacks << args.shift
      end
    end

    # Adds a callback that runs if the
    # export is completed successfully
    def on_success *args, &block
      if block_given?
        @success_callbacks << block
      else
        @success_callbacks << args.shift
      end
    end

    # :nodoc:
    def run_ensure_callbacks
      @ensure_callbacks.each do | ec |
        ec.call(self)
      end
    end

    # :nodoc:
    def run_ignore_callbacks
      @ignore_callbacks.each do | ic |
        ic.call(self)
      end
    end

    # :nodoc:
    def run_success_callbacks
      @success_callbacks.each do | sc |
        sc.call(self)
      end
    end

    # :nodoc:
    def run_failure_callbacks
      @failure_callbacks.each do | fc |
        fc.call(self)
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
        :filepath => @filepath,
        :zipped => @zipped,
        :export_type => @export_type,
        :export_to => @export_to,
        :store_in => @store_in
      }
      return obj.to_json
    end

    ##
    # Final path of the dump file after zip
    ##
    def export_path
      path = @zipped ? @filepath + ".gz" : @filepath
      return path
    end

    ##
    # Writes Json to file using echo
    # file is written to remote server via SSH
    # Echo is used for writing the file
    ##
    def store!
      cmd = "echo \"#{self.to_json.gsub("\"","\\\\\"")}\" > #{@filepath}.json"
      puts cmd
      result = @ssh.exec!(cmd)
      puts result
    end

    def load metadata_path
      json = File.open(metadata_path, "rb").read
      hash = JSON.parse(json)
      set_attributes hash
    end

    def set_attributes options
      options.symbolize_keys!

      @export_time = options[:export_time] if options[:export_time]
      @table = options[:table] if options[:table]
      @database = options[:database] if options[:database]
      @state = options[:state] if options[:state] 
      @incremental_col = options[:incremental_col] if options[:incremental_col]
      @incremental_val = options[:incremental_val] if options[:incremental_val]
      @export_id = options[:export_id] if options[:export_id]
      @filepath = options[:filepath].nil? ? nil : options[:filepath]
      @on_duplicate = options[:on_duplicate] if options[:on_duplicate]
      @export_type = options[:export_type] if options[:export_type]
      @zipped = options[:zipped].nil? ? false : options[:zipped]
      @store_in = options[:store_in] if options[:store_in]
      @export_to = options[:export_to] if options[:export_to]
      @ssh = nil

      @success_callbacks = []
      @failure_callbacks = []
      @ensure_callbacks = []
      @ignore_callbacks = []
    end

  end
end
