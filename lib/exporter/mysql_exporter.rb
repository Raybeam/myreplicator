module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
    end
    
    ##
    # Gets an Export object and dumps the data 
    # Initially using mysqldump
    # Incrementally using mysql -e afterwards
    ##
    def export_table export_obj
      @export_obj = export_obj

      ExportMetadata.record(:table => @export_obj.table_name,
                            :database => @export_obj.source_schema,
                            :export_to => load_to,
                            :export_id => @export_obj.id,
                            :filepath => filepath,
                            :store_in => @export_obj.s3_path,
                            :incremental_col => @export_obj.incremental_column) do |metadata|

        prepare metadata

        if (@export_obj.export_type? == :new && load_to == "mysql") || load_to == "mysql"
          on_failure_state_trans(metadata, "new") # If failed, go back to new
          on_export_success(metadata)
          initial_export metadata
        elsif @export_obj.export_type? == :incremental || load_to == "vertica"
          on_failure_state_trans(metadata, "failed") # Set state trans on failure
          on_export_success(metadata)
          incremental_export_into_outfile metadata
        end
        
      end # metadata
    end

    def load_to
      if @export_obj.export_to == "vertica"
        return "vertica"
      else
        return "mysql"
      end
    end

    ##
    # Setups SSH connection to remote host
    ##
    def prepare metadata
      ssh = @export_obj.ssh_to_source
      metadata.ssh = ssh
    end
      
    def update_export *args
      options = args.extract_options!
      @export_obj.update_attributes! options
    end

    ##
    # File path on remote server
    ##
    def filepath
      File.join(Myreplicator.configs[@export_obj.source_schema]["ssh_tmp_dir"], @export_obj.filename)
    end

    ##
    # Exports Table using mysqldump. This method is invoked only once.
    # Dumps with create options, no need to create table manaully
    ##

    def initial_export metadata
      metadata.export_type = "initial"
      max_value = @export_obj.max_value if @export_obj.incremental_export?
      cmd = initial_mysqldump_cmd
      exporting_state_trans # mark exporting

      puts "Exporting..."
      result = execute_export(cmd, metadata)
      check_result(result, 0)

      @export_obj.update_max_val(max_value) if @export_obj.incremental_export?
    end

    def initial_mysqldump_cmd
      flags = ["create-options", "single-transaction"]       
      cmd = ""
      # Mysql - Mysql Export
      if @export_obj.export_to == "destination_db"
        cmd = SqlCommands.mysqldump(:db => @export_obj.source_schema,
                                    :flags => flags,
                                    :filepath => filepath,
                                    :table_name => @export_obj.table_name)     
      else # Other destinations
        cmd = SqlCommands.mysql_export_outfile(:db => @export_obj.source_schema,
                                               :filepath => filepath,
                                               :table => @export_obj.table_name)
      end
      puts cmd
      return cmd
    end
    
    ##
    # Exports table incrementally, similar to incremental_export method
    # Dumps file in tmp directory specified in myreplicator.yml
    # Note that directory needs 777 permissions for mysql to be able to export the file
    # Uses \\0 as the delimiter and new line for lines
    ##

    def incremental_export_into_outfile metadata
      unless @export_obj.is_running?
        
        if @export_obj.export_type == "incremental"
          max_value = @export_obj.max_value
          metadata.export_type = "incremental"        
          @export_obj.update_max_val if @export_obj.max_incremental_value.blank?   
        end
        if (@export_obj.export_type == "all" && @export_obj.export_to == "vertica")
          metadata.export_type = "incremental"
        end

        options = {
          :db => @export_obj.source_schema,
          :source_schema => @export_obj.source_schema,
          :table => @export_obj.table_name,
          :filepath => filepath,
          :destination_schema => @export_obj.destination_schema,
          :enclosed_by => Myreplicator.configs[@export_obj.source_schema]["enclosed_by"],
          :export_id => @export_obj.id
        }

        schema_status = Myreplicator::MysqlExporter.schema_changed?(options)
        Kernel.p "===== schema_status ====="
        Kernel.p schema_status
        if schema_status[:changed] # && new?
          metadata.export_type = "initial"
        else
          options[:incremental_col] = @export_obj.incremental_column
          options[:incremental_col_type] = @export_obj.incremental_column_type
          options[:export_type] = @export_obj.export_type
          options[:incremental_val] = [@export_obj.destination_max_incremental_value, @export_obj.max_incremental_value].min
          #options[:incremental_val] = @export_obj.max_incremental_value
        end

        #Kernel.p "===== incremental_export_into_outfile OPTIONS ====="
        #Kernel.p options
        cmd = SqlCommands.mysql_export_outfile(options)
        #Kernel.p "===== incremental_export_into_outfile CMD ====="
        #puts cmd      
        exporting_state_trans
        puts "Exporting..."
        result = execute_export(cmd, metadata)
        check_result(result, 0)

        if @export_obj.export_type == "incremental"
          metadata.incremental_val = max_value # store max val in metadata
          @export_obj.update_max_val(max_value) # update max value if export was successful
        end
      end

      return false
    end

    def self.compare_schemas vertica_schema, mysql_schema
      #Kernel.p vertica_schema
      #Kernel.p mysql_schema
      if vertica_schema.size != mysql_schema.size
        return true
      else
        index = 0
        while index < vertica_schema.size
          # check for column name
          if vertica_schema.rows[index][:column_name] != mysql_schema[index]["column_name"]
            puts "diff"
            return true
          end
  
          # check for column's data type
          if compare_datatypes index, vertica_schema, mysql_schema
            puts "diff #{index}"
            return true
          end
          # and others ?? (PRIMARY, DEFAULT NULL, etc.)
          index += 1
        end
      end
      return false      
    end

    def self.compare_datatypes index, vertica_schema, mysql_schema
      type = Myreplicator::VerticaTypes.convert mysql_schema[index]["data_type"], mysql_schema[index]["column_type"]
      if vertica_schema.rows[index][:data_type].downcase != type.downcase
        if !(vertica_schema.rows[index][:data_type].include?("timestamp")) && 
          !(vertica_schema.rows[index][:data_type].include?("decimal")) && 
          !(vertica_schema.rows[index][:data_type].include?("numeric")) &&
          !(vertica_schema.rows[index][:data_type].include?("binary"))
          return true
        end
        return false
      end
      return false
    end
    
    def self.get_mysql_schema_rows mysql_schema 
      mysql_schema_simple_form = []
      mysql_schema.each(:as => :hash) do |row|
        mysql_schema_simple_form << row
      end
      return mysql_schema_simple_form
    end

    def self.schema_changed? options
      #Kernel.p "===== schema_changed? ====="
      #puts options
      mysql_schema = Loader.mysql_table_definition(options)
      vertica_schema = VerticaLoader.destination_table_vertica(options)

      # empty result set from vertica means table does not exist 
      unless vertica_schema.size > 0 
        return {:changed => true, :mysql_schema => mysql_schema, :new => true}
      end
      # compare two schemas
      
      
      mysql_schema_2 = get_mysql_schema_rows mysql_schema
      if compare_schemas(vertica_schema, mysql_schema_2)
        result =  {:changed => true, :mysql_schema => mysql_schema, :vertica_schema => vertica_schema,:new => false}
      else
        result =  {:changed => false, :mysql_schema => mysql_schema}
      end
      #Kernel.p result
      return result
    end

    ##
    # Checks the returned resut from SSH CMD
    # Size specifies if there should be any returned results or not
    ##
    def check_result result, size
      unless result.nil?
        raise Exceptions::ExportError.new("Export Error\n#{result}") if result.length > 0
      end     
    end

    ##
    # Executes export command via ssh on the source DB
    # Updates/interacts with the metadata object
    ##
    def execute_export cmd, metadata
      metadata.store!
      result = ""

      # Execute Export command on the source DB server
      result = metadata.ssh.exec!(cmd)
      
      return result
    end

    ##
    # zips the file on the source DB server
    ##
    def zipfile metadata
      cmd = "cd #{Myreplicator.configs[@export_obj.source_schema]["ssh_tmp_dir"]}; gzip #{@export_obj.filename}"

      puts cmd

      zip_result = metadata.ssh.exec!(cmd)

      unless zip_result.nil?        
        raise Exceptions::ExportError.new("Export Error\n#{zip_result}") if zip_result.length > 0
      end

      metadata.zipped = true

      return zip_result
    end

    def on_failure_state_trans metadata, state
      metadata.on_failure do |m|
        update_export(:state => state, 
                      :export_finished_at => Time.now, 
                      :error => metadata.error)
        begin              
          Myreplicator::Loader.cleanup metadata
        rescue Exception => e
          puts e.message
        end
        raise Exceptions::ExportError.new(metadata.error)
      end
    end

    def exporting_state_trans
      update_export(:state => "exporting", 
                    :export_started_at => Time.now, 
                    :exporter_pid => Process.pid)
    end

    def on_export_success metadata
      metadata.on_success do |m|
        zipfile(metadata)
        update_export(:state => "export_completed", 
                      :export_finished_at => Time.now, 
                      :error => metadata.error)
        metadata.state = "export_completed"
      end
    end
    
  end
end
