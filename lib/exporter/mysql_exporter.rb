module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
      Dir.mkdir(Myreplicator.app_root) unless File.directory?(Myreplicator.app_root)
    end

    def export_table export_obj
      flags = []
      if export_obj.state.blank? || export_obj.state == "new"
      else
        
      end
    end

    def initial_export
      flags = ["create-options", "single-transaction"]       
      SqlCommands.mysqldump(:db => export_obj.source_schema,
                            :flags => flags,
                            :filepath => File.join(Configuration.tmp_path, export_obj.filename))     
    end

    def incremental_export
      
    end

    def dump_export
      
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
