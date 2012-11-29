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
      initials = []
      incrementals = []

      metadata_files.each do |metadata|
        if metadata.export_type == "initial"
          initials << metadata
        else
          incrementals << metadata
        end
      end
      
      initials.each{|metadata| puts metadata.table; initial_load metadata}

      incrementals.each{|metadata| puts metadata.table; incremental_load metadata}

    end

    def initial_load metadata
      exp = Export.find(metadata.export_id)

      zip_result = unzip(metadata.filename)
      raise Exceptions::LoaderError.new("Unzipping #{metadata.filename} Failed!") unless zip_result
      metadata.zipped = false

      cmd = ImportSql.initial_load(:db => exp.destination_schema,
                                   :filepath => File.join(tmp_dir,metadata.filename))      
      puts cmd
      result = `#{cmd}` # execute
      puts result
      
    end
    
    ##
    # Unzips file
    # Checks if the file exists or already unzipped
    ##
    def unzip filename
      cmd = "cd #{tmp_dir}; gunzip #{filename}"

      if File.exist?(File.join(tmp_dir,filename))
        result = `#{cmd}`
        unless result.nil? 
          puts result
          if result.length > 0
            return false 
          end
          return true
        else
          return true
        end
      elsif File.exist?(File.join(tmp_dir,filename.gsub(".gz","")))
        puts "File already unzipped"
        return true
      end

      return false
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
