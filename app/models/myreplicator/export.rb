require 'exporter'

module Myreplicator
  class Export < ActiveRecord::Base
    attr_accessible(:source_schema, 
                    :destination_schema, 
                    :table_name, 
                    :incremental_column, 
                    :incremental_column_type, 
                    :max_incremental_value, 
                    :export_to, 
                    :export_type,
                    :s3_path,
                    :cron, 
                    :last_run,
                    :state,
                    :error,
                    :active,
                    :export_started_at,
                    :export_finished_at,
                    :load_started_at,
                    :load_finished_at,
                    :transfer_started_at,
                    :transfer_finished_at,
                    :exporter_pid,
                    :transporter_pid,
                    :loader_pid
                    )

    attr_reader :filename

    @queue = :myreplicator_export # Provided for Resque

    ##
    # Perfoms the export job, Provided for Resque
    ##
    def self.perform(export_id, *args)
      options = args.extract_options!
      ActiveRecord::Base.verify_active_connections!
      ActiveRecord::Base.connection.reconnect!
      export_obj = Export.find(export_id)

      if export_obj.active
        export_obj.export
      end

    end

    def reload
      Log.run(:job_type => "export", :name => schedule_name, 
      :file => filename, :export_id => id) do |log|
        # TRUNCATE TABLE & Rest incremental value if there is any
        sql = "TRUNCATE TABLE '#{@export.destination_schema}'.'#{@export.table_name}';"
        if self.export_to == "vertica"
          Myreplicator::DB.exec_sql("vertica",sql)
        else
          Myreplicator::DB.exec_sql("#{@export.destination_schema}",sql)
        end
        
        if self.export_type != "all"
          self.max_incremental_value = nil
          self.save!
        end
        
        exporter = MysqlExporter.new
        exporter.export_table self # pass current object to exporter
      end
    end 
    ##
    # Runs the export process using the required Exporter library
    ##
    def export
      Log.run(:job_type => "export", :name => schedule_name, 
              :file => filename, :export_id => id) do |log|
        exporter = MysqlExporter.new
        exporter.export_table self # pass current object to exporter
      end
    end
    
    # def source_mysql_schema
    #   sql = "describe #{source_schema}.#{table_name}"
    #   result = exec_on_source(sql)
    #   return result
    # end

    # def destination_schema_mysql
      
    # end

    def export_type?
      if state == "new"
        return :new
      elsif incremental_export?
        return :incremental
      end
    end

    def incremental_export?
      if export_type == "incremental"
        return true
      end
      return false
    end

    def filename
      @file_name ||= "#{source_schema}_#{table_name}_#{Time.now.to_i}.tsv"
    end
    
    def destination_max_incremental_value      
      if self.export_to == 'vertica'
        sql = SqlCommands.max_value_vsql(:incremental_col => self.incremental_column,
                                              :max_incremental_value => self.max_incremental_value,
                                              :db => self.destination_schema,
                                              :incremental_col_type => self.incremental_column_type,
                                              :table => self.table_name)
        puts sql
        begin
          result = Myreplicator::DB.exec_sql('vertica',sql)
          if result.rows.first[:max].blank?
            return "0"
          else
            case result.rows.first[:max].class.to_s
            when "DateTime"
              return result.rows.first[:max].strftime('%Y-%m-%d %H:%M:%S')
            else
              return result.rows.first[:max].to_s
            end
          end
        rescue Exception => e
          puts "Vertica Table Not Existed"
        end
      else
        begin
          sql = SqlCommands.max_value_sql(:incremental_col => self.incremental_column,
                                                :max_incremental_value => self.max_incremental_value,
                                                :db => self.source_schema,
                                                :incremental_col_type => self.incremental_column_type,
                                                :table => self.table_name)
          puts sql
          result = Myreplicator::DB.exec_sql(self.destination_schema,sql)
          if result.first.nil?
            return "0"
          else
            return result.first.first
          end
        end
      end
      return "0"
    end
    
    def max_value
      sql = SqlCommands.max_value_sql(:incremental_col => self.incremental_column,
                                      :max_incremental_value => self.max_incremental_value,
                                      :db => self.source_schema,
                                      :incremental_col_type => self.incremental_column_type,
                                      :table => self.table_name)
      result = exec_on_source(sql)
      if result.first.nil?
        return "0"
      else
        return result.first.first
      end
    end

    def update_max_val(max_val = nil)
      if max_val.nil?
        self.max_incremental_value = max_value
      else
        self.max_incremental_value = max_val
        self.save!
      end
    end

    def exec_on_source sql
      result = SourceDb.exec_sql(self.source_schema, sql)
      return result
    end

    def ssh_to_source
      puts "Connecting SSH..."
      return connection_factory(:ssh) 
    end

    def sftp_to_source
      puts "Connecting SFTP..."
      return connection_factory(:sftp)
    end
    
    ##
    # Connects to the server via ssh/sftp
    ##
    def connection_factory type
      config = Myreplicator.configs[self.source_schema]
      puts self.source_schema
      puts config
      case type
      when :ssh
        if config.has_key? "ssh_password"
          return Net::SSH.start(config["ssh_host"], config["ssh_user"], :password => config["ssh_password"])

        elsif(config.has_key? "ssh_private_key")
          return Net::SSH.start(config["ssh_host"], config["ssh_user"], :keys => [config["ssh_private_key"]])
        end
      when :sftp
        if config.has_key? "ssh_password"
          return Net::SFTP.start(config["ssh_host"], config["ssh_user"], :password => config["ssh_password"])

        elsif(config.has_key? "ssh_private_key")
          return Net::SFTP.start(config["ssh_host"], config["ssh_user"], :keys => [config["ssh_private_key"]])
        end          
      end
    end

    ##
    # Returns a hash of {DB_NAME => [TableName1,...], DB => ...}
    ##
    def self.available_tables
      metadata = {}
      available_dbs.each do |db|
        tables = SourceDb.get_tables(db)
        metadata[db] = tables
      end
      return metadata
    end

    ##
    # List of all avaiable databases from database.yml file
    # All Export/Load jobs can use these databases
    ##
    def self.available_dbs
      dbs = ActiveRecord::Base.configurations.keys
      available = [] 

      dbs.each do |db|
        db_config = ActiveRecord::Base.configurations[db]
        unless db_config["myreplicator"].nil?
          available << db if db_config["myreplicator"]
        end
      end
      return available
    end

    ##
    # NOTE: Provided for Resque use
    # Schedules all the exports in resque
    # Requires Resque Scheduler
    ##
    def self.schedule_in_resque
      exports = Export.find(:all)
      exports.each do |export|
        if export.active
          export.schedule
        else
          Resque.remove_schedule(export.schedule_name)
        end
      end
      Resque.reload_schedule! # Reload all schedules in Resque
    end

    ##
    # Name used for the job in Resque
    ##
    def schedule_name
      name = "#{source_schema}_#{destination_schema}_#{table_name}"
    end

    ##
    # Schedules the export job in Resque
    ##
    def schedule
      Resque.set_schedule(schedule_name, {
                            :cron => cron,
                            :class => "Myreplicator::Export",
                            :queue => "myreplicator_export",
                            :args => id
                          })
    end

    ##
    # Throws ExportIgnored if the job is still running
    # Checks the state of the job using PID and state
    ##
    def is_running?
      return false if state != "exporting"
      begin
        Process.getpgid(exporter_pid)
        raise Exceptions::ExportIgnored.new("Ignored")
      rescue Errno::ESRCH
        return false
      end
    end

    ##
    # Inner Class that connects to the source database 
    # Handles connecting to multiple databases
    ##

    class SourceDb < ActiveRecord::Base

      def self.connect db
        establish_connection(ActiveRecord::Base.configurations[db])
      end

      ##
      # Returns tables as an Array
      # releases the connection
      ##
      def self.get_tables(db)
        tables = []
        begin
          self.connect(db)
          tables = self.connection.tables  
          self.connection_pool.release_connection
        rescue Mysql2::Error => e
          puts "Connection to #{db} Failed!"
          puts e.message
        end          
        return tables
      end
      
      def self.exec_sql source_db,sql
        SourceDb.connect(source_db)
        return SourceDb.connection.execute(sql)
      end
    end
      
  end
end
