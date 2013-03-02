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

        if @export_obj.export_type? == :new && load_to == "mysql"
          on_failure_state_trans(metadata, "new") # If failed, go back to new
          on_export_success(metadata)
          initial_export metadata
        elsif @export_obj.export_type? == :incremental or load_to == "vertica"
          on_failure_state_trans(metadata, "failed") # Set state trans on failure
          on_export_success(metadata)
          incremental_export metadata
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
      
      return cmd
    end
    
    ##
    # Exports table incrementally, using the incremental column specified
    # If column is not specified, it will export the entire table 
    # Maximum value of the incremental column is recorded BEFORE export starts
    ##

    def incremental_export metadata
      unless @export_obj.is_running?
        max_value = @export_obj.max_value
        metadata.export_type = "incremental"
        @export_obj.update_max_val if @export_obj.max_incremental_value.blank?   
        
        cmd = incremental_export_cmd 
        exporting_state_trans # mark exporting
        puts "Exporting..."
        result = execute_export(cmd, metadata)
        check_result(result, 0)
        metadata.incremental_val = max_value # store max val in metadata
        @export_obj.update_max_val(max_value) # update max value if export was successful
      end
      return false
    end

    def incremental_export_cmd
      sql = SqlCommands.export_sql(:db => @export_obj.source_schema,
                                   :table => @export_obj.table_name,
                                   :incremental_col => @export_obj.incremental_column,
                                   :incremental_col_type => @export_obj.incremental_column_type,
                                   :incremental_val => @export_obj.max_incremental_value)      
      
      cmd = SqlCommands.mysql_export(:db => @export_obj.source_schema,
                                     :filepath => filepath,
                                     :sql => sql)
      
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
        max_value = @export_obj.max_value
        metadata.export_type = "incremental_outfile"        
        @export_obj.update_max_val if @export_obj.max_incremental_value.blank?   
      
        options = {
          :db => @export_obj.source_schema,
          :table => @export_obj.table_name,
          :filepath => filepath,
          :destination_schema => @export_obj.destination_schema}

        unless schema_changed?(options)[:changed]
          options[:incremental_col] = @export_obj.incremental_column
          options[:incremental_col_type] = @export_obj.incremental_column_type
          options[:incremental_val] = @export_obj.max_incremental_value
        end

        cmd = SqlCommands.mysql_export_outfile(options)      
        exporting_state_trans
        puts "Exporting..."
        result = execute_export(cmd, metadata)
        check_result(result, 0)
        metadata.incremental_val = max_value # store max val in metadata
        @export_obj.update_max_val(max_value) # update max value if export was successful
      end
      return false
    end

    def self.compare_schemas vertica_schema, mysql_schema
      if vertica_schema.size != mysql_schema.size
        return {:changed => true, :mysql_schema => mysql_schema, :vertica_schema => vertica_schema,:new => false}
      else
        index = 0
        while index < vertica_schema.size
          # puts vertica_schema.rows[index][:column_name]  + " " + mysql_schema[index]["column_name"]
          # check for column name
          if vertica_schema.rows[index][:column_name] != mysql_schema[index]["column_name"]
            return true
          end
  
          # puts vertica_schema.rows[index][:data_type]  + " " + VerticaTypes.convert(mysql_schema[index]["data_type"],mysql_schema[index]["column_type"])
          # check for column's data type
          if (vertica_schema.rows[index][:data_type] != VerticaTypes.convert(mysql_schema[index]["data_type"],mysql_schema[index]["column_type"]) and vertica_schema.rows[index][:data_type] != "timestamp")
            return true
          end
          # and others ?? (PRIMARY, DEFAULT NULL, etc.)
          index += 1
        end
      end
      return false      
    end
    
    def self.schema_changed? options
      puts options
      mysql_schema = Loader.mysql_table_definition(options)
      vertica_schema = VerticaLoader.destination_table_vertica(options)

      # empty result set from vertica means table does not exist 
      unless vertica_schema.size > 0 
        return {:changed => true, :mysql_schema => mysql_schema, :new => true}
      end
      
      # compare two schemas
      mysql_schema_simple_form = []
      mysql_schema.each(:as => :hash) do |row|
        mysql_schema_simple_form << row  
      end
      
      mysql_schema_2 = mysql_schema_simple_form
      if compare_schemas(vertica_schema, mysql_schema_2)
        result =  {:changed => true, :mysql_schema => mysql_schema, :vertica_schema => vertica_schema,:new => false}
      else
        result =  {:changed => false}
      end
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
      end
    end

    def exporting_state_trans
      update_export(:state => "exporting", 
                    :export_started_at => Time.now, 
                    :exporter_pid => Process.pid)
    end

    def on_export_success metadata
      metadata.on_success do |m|
        update_export(:state => "export_completed", 
                      :export_finished_at => Time.now, 
                      :error => metadata.error)
        metadata.state = "export_completed"
        zipfile(metadata)
      end
    end
    
  end
end
