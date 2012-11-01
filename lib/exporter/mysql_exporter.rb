module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
      Dir.mkdir(Myreplicator.app_root) unless File.directory?(Myreplicator.app_root)
    end

    def export_table
      
    end

    def create_table
      Myreplicator::SqlCommands.mysqldump(:db => db, 
                                          :flags => ["create-options", "compact"],
                                          :filepath => filepath)
    end

    def zipfile
      cmd = "cd #{Myreplicato::Configuration.tmp_path}; gzip #{filename}"
      puts cmd
      `#{cmd}`
    end
    
  end
end
