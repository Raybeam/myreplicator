module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
      
    end

    def export_table
      
    end

    def create_table
      Myreplicator::SqlCommands.mysqldump(:db => db, 
                                          :flags => ["create-options", "compact"],
                                          :filepath => filepath)
    end

    def zipfile
      cmd = "cd gzip #{filepath}"
      ` `
    end

  end
end
