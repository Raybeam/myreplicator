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
          initial_load metadata
        else
          incremental_load metadata
        end

        metadata
      end
    end

    def initial_load metadata
      exp = Export.find(metadata.export_id)

      unzip(metadata.filename)
      # metadata.zipped = false
      # metadata.store!
      
      cmd = ImportSql.initial_load(:db => exp.destination_schema,
                                   :filepath => File.join(tmp_dir,metadata.filename))
      

      puts metadata.filename
      puts cmd
    end

    def unzip filename
      cmd = "cd #{tmp_dir}; gunzip #{filename}"
      puts cmd
      return `#{cmd}`
    end

    def incremental_load metadata
      exp = Export.find(metadata.export_id)
      cmd = ImportSql.load_data_infile(:table_name => exp.table_name, 
                                       :db => exp.destination_schema,
                                       :filename => metadata.filename
                                       )

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
