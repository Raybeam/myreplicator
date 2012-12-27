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
    
    def export
      exporter = MysqlExporter.new
      exporter.export_table self    
    end

    def filename
      @file_name ||= "#{source_schema}_#{table_name}_#{Time.now.to_i}.tsv"
    end

    def max_value
      sql = SqlCommands.max_value_sql(:incremental_col => self.incremental_column,
                                      :db => self.source_schema,
                                      :table => self.table_name)
      result = exec_on_source(sql)

      return result.first.first
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

    def connection_factory type
      case type
      when :ssh
        if Myreplicator.configs[self.source_schema].has_key? "ssh_password"
          return Net::SSH.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                                Myreplicator.configs[self.source_schema]["ssh_user"],
                                :password => Myreplicator.configs[self.source_schema]["ssh_password"])

        elsif(Myreplicator.configs[self.source_schema].has_key? "ssh_private_key")
          return Net::SSH.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                                Myreplicator.configs[self.source_schema]["ssh_user"],
                                :keys => [Myreplicator.configs[self.source_schema]["ssh_private_key"]])        
        end
      when :sftp
        if Myreplicator.configs[self.source_schema].has_key? "ssh_password"
          return Net::SFTP.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                                 Myreplicator.configs[self.source_schema]["ssh_user"],
                                 :password => Myreplicator.configs[self.source_schema]["ssh_password"])

        elsif(Myreplicator.configs[self.source_schema].has_key? "ssh_private_key")
          return Net::SFTP.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                                 Myreplicator.configs[self.source_schema]["ssh_user"],
                                 :keys => [Myreplicator.configs[self.source_schema]["ssh_private_key"]])
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

    def self.available_dbs
      dbs = ActiveRecord::Base.configurations.keys
      dbs.delete("development")
      dbs.delete("production")
      dbs.delete("test")
      return dbs
    end

    ##
    # Inner Class that connects to the source database 
    # Handles connecting to multiple databases
    ##

    # BOB : Not sure what I think of inner classes
    # Is this class used anywhere else in the app?
    # If so, I'd namespace it in it own file (e.g. Export::SourceDb)
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
