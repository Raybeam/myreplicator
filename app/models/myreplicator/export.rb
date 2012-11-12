require 'exporter'

module Myreplicator
  class Export < ActiveRecord::Base
    attr_accessible(:source_schema, 
                    :destination_schema, 
                    :table_name, 
                    :incremental_column, 
                    :max_incremental_value, 
                    :export_to, 
                    :export_type,
                    :s3_path,
                    :cron, 
                    :last_run,
                    :state,
                    :error,
                    :active)

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
      Kernel.p result.first.first

      return result.first.first
    end

    def update_max_val
      self.max_incremental_value = max_value
      self.save!
    end

    def exec_on_source sql
      result = SourceDb.exec_sql(self.source_schema, sql)
      return result
    end

    def ssh_to_source
      puts "Connecting SSH..."
      Net::SSH.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                     Myreplicator.configs[self.source_schema]["ssh_user"],
                     Myreplicator.configs[self.source_schema]["ssh_password"]) do |ssh|
        
        puts "SSH connected"

        yield ssh

        ssh.close
      end
    end

    def sftp_to_source
      puts "Connecting SFTP..."
      Net::SFTP.start(Myreplicator.configs[self.source_schema]["ssh_host"],
                      Myreplicator.configs[self.source_schema]["ssh_user"],
                      Myreplicator.configs[self.source_schema]["ssh_password"]) do |sftp|

        puts "SFTP connected"

        yield sftp

        sftp.close
      end
    end

    ##
    # Inner Class that connects to the source database 
    # Handles connecting to multiple databases
    ##

    class SourceDb < ActiveRecord::Base
      
      def self.connect db
        @@connected ||= true
        establish_connection(ActiveRecord::Base.configurations[db])
        Kernel.p ActiveRecord::Base.connected?
      end
      
      def self.exec_sql source_db,sql
        SourceDb.connect(source_db)
        return SourceDb.connection.execute(sql)
      end
    end
      
  end
end
