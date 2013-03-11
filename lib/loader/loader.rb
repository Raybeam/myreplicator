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
    # Reconnection provided for resque workers
    ##
    def self.perform
      ActiveRecord::Base.verify_active_connections!
      ActiveRecord::Base.connection.reconnect!
      load # Kick off the load process
    end

    ##
    # Kicks off all initial loads first and then all incrementals
    # Looks at metadata files stored locally
    # Note: Initials are loaded sequentially
    ##
    def self.load
      initials = []
      incrementals = []
      all_files = Loader.metadata_files

      all_files.each do |m|
        if m.export_type == "initial"
          initials << m # Add initial to the list
          all_files.delete(m) # Delete obj from mixed list

          all_files.each do |md|
            if m.equals(md) && md.export_type == "incremental"
              initials << md # incremental should happen after the initial load
              all_files.delete(md) # remove from current list of files
            end
          end
        end
      end
      
      incrementals = all_files # Remaining are all incrementals
      
      initial_procs = Loader.initial_loads initials
      parallel_load initial_procs

      incremental_procs = Loader.incremental_loads incrementals
      parallel_load incremental_procs
    end
    
    def self.parallel_load procs
      p = Parallelizer.new(:klass => "Myreplicator::Loader")
      procs.each do |proc|
        p.queue << {:params => [], :block => proc}
      end
      
      p.run
    end

    ##
    # Loads all new tables concurrently
    # multiple files 
    ## 
    def self.initial_loads initials
      procs = []

      initials.each do |metadata| 
        procs << Proc.new {
          Log.run(:job_type => "loader", 
                  :name => "#{metadata.export_type}_import", 
                  :file => metadata.filename, 
                  :export_id => metadata.export_id) do |log|

            if Loader.transfer_completed? metadata
              if metadata.export_to == "vertica"
                Loader.incremental_load metadata
              else
                Loader.initial_load metadata
              end
              Loader.cleanup metadata
            end

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
    
              if Loader.transfer_completed? metadata            
                Loader.incremental_load metadata
                Loader.cleanup metadata
              end

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
        incrementals.delete(metadata)

        # look for same loads
        incrementals.each do |md| 
          if metadata.equals(md)
            group << md
            incrementals.delete(md) # remove from main array
          end
        end
        
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
      
      options = {:table_name => exp.table_name, 
        :db => exp.destination_schema,
        :filepath => metadata.destination_filepath(tmp_dir), 
        :source_schema => exp.source_schema,      
        :fields_terminated_by => "\\0",
        :lines_terminated_by => "\\n"}
      
      case metadata.export_to 
      when "vertica"
        Loader.load_to_vertica options, metadata, exp
      when "mysql"
        cmd = ImportSql.load_data_infile(options)
        puts cmd
        result = `#{cmd}` # execute
        unless result.nil?
          if result.size > 0
            raise Exceptions::LoaderError.new("Incremental Load #{metadata.filename} Failed!\n#{result}") 
          end
        end
      end #case  
    end

    ##
    # Load to Vertica
    ##
    def self.load_to_vertica options, metadata, exp
      options = {:table_name => exp.table_name, 
        :db => ActiveRecord::Base.configurations["vertica"]["database"],
        :filepath => metadata.destination_filepath(tmp_dir), 
        :source_schema => exp.source_schema, :export_id => metadata.export_id,
        :metadata => metadata
      }
      
      options[:destination_schema] = exp.destination_schema
      
      result = Myreplicator::VerticaLoader.load options
      
      ##TO DO: Handle unsuccessful vertica loads here

    end

    ##
    # Returns true if the transfer of the file
    # being loaded is completed
    ##
    def self.transfer_completed? metadata
      if Log.completed?(:export_id => metadata.export_id,
                        :file => metadata.destination_filepath(tmp_dir),
                        :job_type => "transporter")
        return true
      end
      return false
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

    ##
    # Clears files that are older than the passed metadata file.
    # Note: This methoded is provided to ensure no old incremental files
    # ever get loaded after the schema change algorithm has been applied 
    ##
    def self.clear_older_files metadata
      files = Loader.metadata_files
      max_date = DateTime.strptime metadata.export_finished_at
      files.each do |m|
        if metadata.export_id == m.export_id
          if max_date > DateTime.strptime(m.export_finished_at)
            Loader.cleanup m if metadata.filepath != m.filepath
          end 
        end
      end     
    end

    def self.mysql_table_definition options
      sql = "SELECT table_schema, table_name, column_name, is_nullable, data_type, column_type, column_key "
      sql += "FROM INFORMATION_SCHEMA.COLUMNS where table_name = '#{options[:table]}' "
      sql += "and table_schema = '#{options[:source_schema]}';"
      
      puts sql
      
      desc = DB.exec_sql(options[:source_schema], sql)
      puts desc
      return desc
    end

  end
end
