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
    end
    
    def to_json
      
    end

    def store_json

    end

    def load
      
    end

  end
end
