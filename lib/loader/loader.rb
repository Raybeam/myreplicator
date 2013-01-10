require "exporter"

module Myreplicator
  class Loader
    
    @queue = :myreplicator_load # Provided for Resque
    
    def initialize *args
      options = args.extract_options!
    end
    
    def self.tmp_dir
      @tmp_dir ||= File.join(Myreplicator.app_root,"tmp", "myreplicator")
    end

    ##
    # Main method provided for resque
    ##
    def self.perform
      load # Kick off the load process
    end

    ##
    # Kicks off all initial loads first and then all incrementals
    # Looks at metadata files stored locally
    # Note: Initials are loaded sequentially
    # If there is a 
    ##
    def self.load
      initials = []
      incrementals = []
      metadata = Loader.metadata_files

      metadata.each do |m|
        if m.export_type == "initial"

          initials << m # Add initial to the list
          metadata.delete(m) # Delete obj from mixed list

          metadata.each do |md|
            if m.equals(md) && md.export_type == "incremental"
              initials << md # incremental should happen after the initial load
              metadata.delete(md) # remove from current list of files
            end
          end
        end
      end
      
      incrementals = metadata # Remaining are all incrementals
      
      initial_procs = Loader.initial_loads initials
      Kernel.p initial_procs
      parallel_load initial_procs

      incremental_procs = Loader.incremental_loads incrementals
      Kernel.p incremental_procs
      parallel_load incremental_procs
    end
    
    def self.parallel_load procs
      p = Parallelizer.new(:klass => "Myreplicator::Transporter")
      procs.each do |proc|
        p.queue << {:params => [], :block => proc}
      end
    end

    ##
    # Loads all new tables concurrently
    # multiple files 
    ## 
    def self.initial_loads initials
      procs = []

      initials.each do |metadata| 
        procs << Proc.new {
          puts metadata.table
          Log.run(:job_type => "loader", 
                  :name => "initial_import", 
                  :file => metadata.filename, 
                  :export_id => metadata.export_id) do |log|
            
            Loader.initial_load metadata
            Loader.cleanup metadata
          end
        }
      end

      return procs
    end

    ##
    # Load all incremental files
    # Ensures that multiple loads to the same table
    # happen sequentially.
    ##
    def self.incremental_loads incrementals
      groups = Loader.group_incrementals incrementals
      procs = []
      groups.each do |group|
        procs << Proc.new {
          group.each do |metadata|
            Log.run(:job_type => "loader", 
                    :name => "incremental_import", 
                    :file => metadata.filename, 
                    :export_id => metadata.export_id) do |log|
              
              Loader.incremental_load metadata
              Loader.cleanup metadata
            end
          end # group
        }
      end # groups
      
      return procs
    end

    ##
    # Groups all incrementals files for 
    # the same table together
    # Returns and array of arrays
    # NOTE: Each Arrays should be processed in 
    # the same thread to avoid collision 
    ##
    def self.group_incrementals incrementals
      groups = [] # array of all grouped incrementals

      incrementals.each do |metadata|
        group = [metadata]

        incrementals.each do |md| 
          if metadata.equals(md)
            group << md
            metadata.delete(md) # remove from main array
          end
        end
        
        incrementals.delete(metadata)
        groups << group
      end

      return groups
    end
    
    ##
    # Creates table and loads data
    ##
    def self.initial_load metadata
      exp = Export.find(metadata.export_id)
      Loader.unzip(metadata.filename)
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
    def self.incremental_load metadata
      exp = Export.find(metadata.export_id)
      Loader.unzip(metadata.filename)
      metadata.zipped = false
      
      options = {:table_name => exp.table_name, :db => exp.destination_schema,
        :filepath => metadata.destination_filepath(tmp_dir)}

      if metadata.export_type == "incremental_outfile"
        options[:fields_terminated_by] = ";~;"
        options[:lines_terminated_by] = "\\n"
      end

      cmd = ImportSql.load_data_infile(options)

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
    def self.cleanup metadata
      puts "Cleaning up..."
      FileUtils.rm "#{metadata.destination_filepath(tmp_dir)}.json" # json file
      FileUtils.rm metadata.destination_filepath(tmp_dir) # dump file
    end

    ##
    # Unzips file
    # Checks if the file exists or already unzipped
    ##
    def self.unzip filename
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

    def self.metadata_files
      files = []
      Dir.glob(File.join(tmp_dir, "*.json")).each do |json_file|
        files << ExportMetadata.new(:metadata_path => json_file)
      end
      return files
    end

  end
end
