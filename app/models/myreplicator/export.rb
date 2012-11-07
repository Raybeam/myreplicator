require 'exporter'

module Myreplicator
  class Export < ActiveRecord::Base
    attr_accessible(:source_schema, 
                    :destination_schema, 
                    :table_name, 
                    :incremental_column, 
                    :max_incremental_value, 
                    :export_to, 
                    :export_type,
                    :s3_path,
                    :cron, 
                    :last_run,
                    :state,
                    :error,
                    :active)

    attr_reader :filename
    
    def export
      exporter = MysqlExporter.new      
      exporter.export_table self
    end

    def filename
      @file_name ||= "#{source_schema}_#{table_name}_#{Time.now.to_i}"
    end

  end
end
