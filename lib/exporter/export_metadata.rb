require 'json'

module Myreplicator
  class ExportMetadata

    attr_accessor :export_time, :table, :database, :state, :incremental_col, :export_id, :incremental_val

    def initialize *args
      options = args.extract_options!
      load if options[:json] 
      @export_time = options[:export_time]
      @table = options[:table]
      @database = options[:database]
      @state = options[:state]
      @incremental_col = options[:incremental_col]
      @incremental_val = options[:incremental_val]
      @export_id = options[:export_id]
      @filepath = options[:filepath]
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

    def store_json json
      File.open("#{@filepath}.json", 'w') {|f| f.write(json)}
    end

    def load
      JSON.parse(json)
    end

  end
end
