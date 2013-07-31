require "exporter"

module Myreplicator
  class Loader
    
    @queue = :myreplicator_load # Provided for Resque
     
    
    def initialize *args
      options = args.extract_options!
    end
    
    def self.tmp_dir
      #@tmp_dir ||= File.join(Myreplicator.app_root,"tmp", "myreplicator")
      @tmp_dir ||= Myreplicator.tmp_path
    end

    ##
    # Main method provided for resque
    # Reconnection provided for resque workers
    ##
    def self.perform *args
      options = args.extract_options!
      id = options[:id]
      if id.blank?  
        ActiveRecord::Base.verify_active_connections!
        ActiveRecord::Base.connection.reconnect!
        load # Kick off the load process
      else
        ActiveRecord::Base.verify_active_connections!
        ActiveRecord::Base.connection.reconnect!
        load_id(id)
      end
    end
    
    ##
    # Running loader for 1 export object
    ##
    def load_id id
      
      #Resque.enqueue(Myreplicator::Loader, id)
      #Resque.enqueue(Myreplicator::Export,342)
    end

    ##
    # Kicks off all initial loads first and then all incrementals
    # Looks at metadata files stored locally
    # Note: Initials are loaded sequentially
    ##
    def self.load
      initials = []
      incrementals = []
      all_files = Myreplicator::Loader.metadata_files
      
      files_to_metadata = {}
      @redis = Redis.new(:host => Settings[:redis][:host], :port => Settings[:redis][:port])
      @load_set = "myreplicator_load_set"
      @load_hash = "myreplicator_load_hash"
      # clear out |k,v| of already deleted filed 
      @redis.hgetall(@load_hash).size
      @redis.hgetall(@load_hash).each do |k, v|
        if @redis.hget(@load_hash, k) == '1'
          @redis.hdel(@load_hash, k)
        end
      end
      
      # making the hash for mapping filepath to metadata object
      all_files.each do |m|
        if Myreplicator::Loader.transfer_completed? m
          if !(@redis.hexists(@load_hash, m.filepath))
            @redis.hset(@load_hash, m.filepath, 0)
            @redis.sadd(@load_set, m.filepath)
          end
          files_to_metadata[m.filepath] = m
        else
          # for the fun of commenting: do nothing
        end
      end
      
      # processing the files in "queue"
      while @redis.smembers(@load_set).size > 0
        filepath = @redis.spop(@load_set)
              
        metadata = files_to_metadata[filepath]
        if metadata.blank?
          next
        end
        # init load
        if metadata.export_type == "initial"
          if Myreplicator::Loader.transfer_completed? metadata
            Myreplicator::Log.run(:job_type => "loader",
            :name => "#{metadata.export_type}_import",
            :file => metadata.filename,
            :export_id => metadata.export_id) do |log|
              if metadata.export_to == "vertica"
                Myreplicator::Loader.incremental_load metadata
              else
                Myreplicator::Loader.initial_load metadata
              end
              Myreplicator::Loader.cleanup metadata
            end
            @redis.hset(@load_hash, metadata.filepath, 1)
          else #transporter not done yet, return the file to @load_set 
            @redis.sadd(@load_set, metadata.filepath)
          end
          
        else #if metadata.export_type == "incremental" # incremental load
          if Myreplicator::Loader.transfer_completed? metadata
            Myreplicator::Log.run(:job_type => "loader",
            :name => "incremental_import",
            :file => metadata.filename,
            :export_id => metadata.export_id) do |log|
              Myreplicator::Loader.incremental_load metadata
              Myreplicator::Loader.cleanup metadata
            end
            @redis.hset(@load_hash, metadata.filepath, 1)
          else #transporter not done yet, return the file to @load_set
            @redis.sadd(@load_set, metadata.filepath)
          end
        end
        sleep(2)
      end # end while        
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
          Myreplicator::Log.run(:job_type => "loader", 
                  :name => "#{metadata.export_type}_import", 
                  :file => metadata.filename, 
                  :export_id => metadata.export_id) do |log|

            if Myreplicator::Loader.transfer_completed? metadata
              if metadata.export_to == "vertica"
                Myreplicator::Loader.incremental_load metadata
              else
                Myreplicator::Loader.initial_load metadata
              end
              Myreplicator::Loader.cleanup metadata
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
            Myreplicator::Log.run(:job_type => "loader", 
                    :name => "incremental_import", 
                    :file => metadata.filename, 
                    :export_id => metadata.export_id) do |log|
    
              if Myreplicator::Loader.transfer_completed? metadata            
                Myreplicator::Loader.incremental_load metadata
                Myreplicator::Loader.cleanup metadata
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
      exp = Myreplicator::Export.find(metadata.export_id)
      #Kernel.p "===== unzip ====="
      #Loader.unzip(metadata.filename)
      #metadata.zipped = false
      filename = metadata.filename
      if filename.split('.').last == 'gz'
        filepath = metadata.destination_filepath(tmp_dir)
        cmd = "gunzip #{filepath}"
        system(cmd)
        unzip_file = File.join(tmp_dir, filename.split('.')[0..-2].join('.'))
        cmd = Myreplicator::ImportSql.initial_load(:db => exp.destination_schema,
                                         :filepath => unzip_file.to_s)
        puts cmd
        result = `#{cmd} 2>&1` # execute
        cmd2 = "gzip #{unzip_file.to_s}"
        system(cmd2)
      else
        cmd = Myreplicator::ImportSql.initial_load(:db => exp.destination_schema,
                                         :filepath => metadata.destination_filepath(tmp_dir))
        puts cmd
        result = `#{cmd} 2>&1` # execute
      end
      
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
      exp = Myreplicator::Export.find(metadata.export_id)
      #Loader.unzip(metadata.filename)
      #metadata.zipped = false
      
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
      #Kernel.p "===== transfer_completed? metadata ====="
      #Kernel.p ({:export_id => metadata.export_id,
      #                        :file => metadata.export_path,
      #:job_type => "transporter"})
      if Log.completed?(:export_id => metadata.export_id,
                        :file => metadata.export_path,
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
      e1 = nil
      e2 = nil
      begin
      FileUtils.rm metadata.metadata_filepath(tmp_dir) # json file
      rescue Exception => e
        e1 = e
        puts e.message
      end
      begin
      FileUtils.rm metadata.destination_filepath(tmp_dir) # dump file
      rescue Exception => e
        e2 = e
        puts e.message
      end
      if (!e1.blank?)
        raise Exceptions::LoaderError.new("#{e1.message}")
      end
      if (!e2.blank?)
        raise Exceptions::LoaderError.new("#{e2.message}")
      end
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
        files << Myreplicator::ExportMetadata.new(:metadata_path => json_file)
      end
      result = []
      #Kernel.p files
      files.each do |file|
        job = Export.where("id = #{file.export_id}").first
        #if job.state == "transport_completed"
        result << file
        #end
      end
      return result
    end

    ##
    # Clears files that are older than the passed metadata file.
    # Note: This methoded is provided to ensure no old incremental files
    # ever get loaded after the schema change algorithm has been applied 
    ##
    def self.clear_older_files metadata
      files = Loader.metadata_files
      #Kernel.p "===== clear old files ====="
      #Kernel.p metadata
      #Kernel.p files
      max_date = DateTime.strptime metadata.export_time
      files.each do |m|
        if metadata.export_id == m.export_id
          if max_date > DateTime.strptime(m.export_time)
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
      
      desc = Myreplicator::DB.exec_sql(options[:source_schema], sql)
      puts desc
      return desc
    end

  end
end
