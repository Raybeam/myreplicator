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

    
    
    
  end
end
