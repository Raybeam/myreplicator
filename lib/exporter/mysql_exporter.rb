module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
      @tmp_dir = File.join(Myreplicator.app_root,"tmp", "myreplicator")
      Dir.mkdir(@tmp_dir) unless File.directory?(@tmp_dir)
    end

    def export_table export_obj
      @export_obj = export_obj
      
      if @export_obj.state == "new"
        initial_export
      elsif !@export_obj.incremental_column.blank?
        incremental_export
      end
    end

    def initial_export
      flags = ["create-options", "single-transaction"]       
      cmd = SqlCommands.mysqldump(:db => @export_obj.source_schema,
                                  :flags => flags,
                                  :filepath => File.join(Myreplicator.tmp_path, @export_obj.filename),
                                  :table_name => @export_obj.table_name)     
      result = `#{cmd}`
      raise Exceptions::ExportError.new("Initial Dump error") if result.length > 0
    end

    def incremental_export
      max_value = @export_obj.max_value

      @export_obj.update_max_val if @export_obj.incremental_value.blank?   

      Kernel.p max_value
      Kernel.p @export_obj.incremental_value
      puts "Both must be the same"

      begin
        sql = SqlCommands.export_sql(:db => @export_obj.source_schema,
                                     :table => @export_obj.table_name,
                                     :incremental_col => @export_obj.incremental_column,
                                     :incremental_val => @export_obj.incremental_value)      
        
        SqlCommands.mysql_export(:db => @export_obj.source_schema,
                                 :filepath => File.join(Myreplicator.tmp_path, @export_obj.filename),
                                 :sql => sql)
      end
    end

    def dump_export
      SqlCommands.mysqldump(:db => @export_obj.source_schema,
                            :filepath => File.join(Myreplicator.tmp_path, @export_obj.filename))    
    end

    def create_table
      Myreplicator::SqlCommands.mysqldump(:db => db, 
                                          :flags => ["create-options", "compact"],
                                          :filepath => filepath)
    end

    def zipfile
      cmd = "cd #{Myreplicator.tmp_path}; gzip #{filename}"
      puts cmd
      `#{cmd}`
    end
    
  end
end
