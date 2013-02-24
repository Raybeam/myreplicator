module Myreplicator
  class DB < ActiveRecord::Base
      
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
      DB.connect(source_db)
      return DB.connection.execute(sql)
    end
  end
end
