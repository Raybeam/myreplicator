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
      export_obj.export
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
    
    def schema_changed?
      exec_on_source()
    end

    def source_mysql_schema
      sql = "describe #{source_schema}.#{table_name}"
      result = exec_on_source(sql)
      return result
    end

    def destination_schema_vertica
      sql = "select column_name, data_type From columns where table_name = '#{table_name}' AND table_schema = '#{destination_schema}'"
      puts sql
      result = SourceDb.exec_sql("vertica",sql)
      return result     
    end

    def destination_schema_mysql
      
    end

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

    def max_value
      sql = SqlCommands.max_value_sql(:incremental_col => self.incremental_column,
                                      :db => self.source_schema,
                                      :table => self.table_name)
      result = exec_on_source(sql)

      return result.first.first.to_s(:db)
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
      dbs.delete("development")
      dbs.delete("production")
      dbs.delete("test")
      return dbs
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
