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
      
      initials.each do |metadata| 
        puts metadata.table

        Log.run(:job_type => "loader", 
                :name => "initial_import", 
                :file => metadata.filename, 
                :export_id => metadata.export_id) do |log|
          
          initial_load metadata
        end

        cleanup metadata
      end

      incrementals.each do |metadata|
        puts metadata.table
        Log.run(:job_type => "loader", 
                :name => "incremental_import", 
                :file => metadata.filename, 
                :export_id => metadata.export_id) do |log|

          incremental_load metadata
        end
        cleanup metadata
      end
    end

    ##
    # Creates table and loads data
    ##
    def initial_load metadata
      exp = Export.find(metadata.export_id)
      unzip(metadata.filename)
      metadata.zipped = false

      cmd = ImportSql.initial_load(:db => exp.destination_schema,
                                   :filepath => metadata.destination_filepath(tmp_dir))      
      puts cmd

      result = `#{cmd}` # execute
      unless result.nil?
        if result.size > 0
          raise Exceptions::LoaderError.new("Initial Load #{metadata.filename} Failed!\n#{result}") 
        end
      end
      
    end

    ##
    # Loads data incrementally
    # Uses the values specified in the metadatta object
    ##
    def incremental_load metadata
      exp = Export.find(metadata.export_id)
      unzip(metadata.filename)
      metadata.zipped = false

      cmd = ImportSql.load_data_infile(:table_name => exp.table_name, 
                                       :db => exp.destination_schema,
                                       :filepath => metadata.destination_filepath(tmp_dir))
      puts cmd
      
      result = `#{cmd}` # execute
      
      unless result.nil?
        if result.size > 0
          raise Exceptions::LoaderError.new("Incremental Load #{metadata.filename} Failed!\n#{result}") 
        end
      end

    end

    ##
    # Deletes the metadata file and extract
    ##
    def cleanup metadata
      puts "Cleaning up..."
      FileUtils.rm "#{metadata.destination_filepath(tmp_dir)}.json" # json file
      FileUtils.rm metadata.destination_filepath(tmp_dir) # dump file
    end

    ##
    # Unzips file
    # Checks if the file exists or already unzipped
    ##
    def unzip filename
      cmd = "cd #{tmp_dir}; gunzip #{filename}"
      passed = false
      if File.exist?(File.join(tmp_dir,filename))
        result = `#{cmd}`
        unless result.nil? 
          puts result
          unless result.length > 0
            passed = true
          end
        else
          passed = true
        end
      elsif File.exist?(File.join(tmp_dir,filename.gsub(".gz","")))
        puts "File already unzipped"
        passed = true
      end

      raise Exceptions::LoaderError.new("Unzipping #{filename} Failed!") unless passed
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
