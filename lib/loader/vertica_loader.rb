module Myreplicator
  class VerticaLoader
    class << self 
      def create_table *args
        options = args.extract_options!
        columns = []
        table_definition(options).each(:as => :hash) do |row|
          columns << row
        end
        options[:columns] = columns
        
      sql = VerticaSql.create_table_stmt options
      end
      
      def create_all_tables db
        tables = SourceDb.get_tables(db)
        tables.each do |table|
          puts "Creating #{db}.#{table}"
          sql = create_table(:vertica_db => "bidw", 
                             :vertica_table => table, 
                             :vertica_schema => db,
                           :table => table,
                             :db => db)
          VerticaDb::Base.connection.execute sql
        end
      end
      
      def load_to_vertica options
        cmd = "/opt/vertica/bin/vsql -hsfo-load-dw-01 -Ubiapp -wbiapp123 -c #{options[:sql]}"
        puts cmd
        result = `#{cmd}`
        Kernel.p result
      end
      
      def ssh_connection options
        ssh = Net::SSH.start(options[:ssh_host], options[:ssh_user], :password => options[:ssh_password])
      end
      
      def table_definition options
        sql = "SELECT table_schema, table_name, column_name, is_nullable, data_type, column_type, column_key "
        sql += "FROM INFORMATION_SCHEMA.COLUMNS where table_name = '#{options[:table]}' "
        sql += "and table_schema = '#{options[:db]}';"
        
        puts sql
      
        desc = SourceDb.exec_sql(options[:db], sql)
        
        return desc
      end
    end
    
  end
end
