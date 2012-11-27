require "exporter"

module Myreplicator
  class Loader
    
    def initialize *args
      options = args.extract_options!
    end
    
    def tmp_dir
      @tmp_dir ||= File.join(Myreplicator.app_root,"tmp", "myreplicator")
    end

    def load
      metadata_files.each do |metadata|
        puts metadata.export_type
        if metadata.export_type == "initial"
          initial_load
        else
          incremental_load
        end

        metadata
      end
    end

    def initial_load metadata
      exp = Export.find(metadata.export_id)
      
      cmd = ImportSql.load_data_infile(:table_name => exp.table_name, 
                                       :db => exp.destination_schema)
      
      metadata.filename
    end
    
    def incremental_load metadata

    end

    def metadata_files
      files = []
      Dir.glob(File.join(tmp_dir, "*.json")).each do |json_file|
        files << ExportMetadata.new(:metadata_path => json_file)
      end
      return files
    end

  end
end
