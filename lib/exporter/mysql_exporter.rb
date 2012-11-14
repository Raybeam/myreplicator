module Myreplicator
  class MysqlExporter
    
    def initialize *args
      options = args.extract_options!
      @tmp_dir = File.join(Myreplicator.app_root,"tmp", "myreplicator")
      Dir.mkdir(@tmp_dir) unless File.directory?(@tmp_dir)
    end

    def export_table export_obj
      @export_obj = export_obj

      ExportMetadata.record(:table => @export_obj.table_name,
                            :database => @export_obj.source_schema,
                            :filepath => filepath) do |metadata|

        if @export_obj.state == "new"
          update_export(:state => "running")
          initial_export metadata
        elsif !@export_obj.incremental_column.blank?
          update_export(:state => "running") || @export_obj.state == "failed"
          incremental_export metadata
        end

      end
    end

    def update_export *args
      options = args.extract_options!
      @export_obj.update_attributes! options
    end

    ##
    # File path on remote server
    ##
    def filepath
      File.join(Myreplicator.configs[@export_obj.source_schema]["ssh_tmp_dir"], @export_obj.filename)
    end

    ##
    # Exports Table using mysqldump. This method is invoked only once.
    # Dumps with create options, no need to create table manaully
    ##

    def initial_export metadata
      flags = ["create-options", "single-transaction"]       
      cmd = SqlCommands.mysqldump(:db => @export_obj.source_schema,
                                  :flags => flags,
                                  :filepath => filepath,
                                  :table_name => @export_obj.table_name)     

      metadata.on_failure do |m|
        update_export(:state => "failed", :export_finished_at => Time.now)
      end
        
      update_export(:state => "exporting", :export_started_at => Time.now)

      result = execute_export(cmd, metadata)

      update_export(:state => "export_completed", :export_finished_at => Time.now)

      unless result.nil?
        raise Exceptions::ExportError.new("Initial Dump error") if result.length > 0
      end
    end

    ##
    # Exports table incrementally, using the incremental column specified
    # If column is not specified, it will export the entire table 
    # Maximum value of the incremental column is recorded BEFORE export starts
    ##

    def incremental_export metadata
      max_value = @export_obj.max_value

      @export_obj.update_max_val if @export_obj.incremental_value.blank?   

      Kernel.p max_value
      Kernel.p @export_obj.incremental_value
      puts "Both must be the same"
      sql = SqlCommands.export_sql(:db => @export_obj.source_schema,
                                   :table => @export_obj.table_name,
                                   :incremental_col => @export_obj.incremental_column,
                                   :incremental_val => @export_obj.incremental_value)      
      cmd = SqlCommands.mysql_export(:db => @export_obj.source_schema,
                                     :filepath => filepath,
                                     :sql => sql)
      
      
      update_export(:state => "exporting", :export_started_at => Time.now)
      
      metadata.on_failure do |m|
        update_export(:state => "failed", :export_finished_at => Time.now)
      end
        
      result = execute_export(cmd, metadata)
      
      update_export(:state => "export_completed", :export_finished_at => Time.now)

      unless result.nil?
        raise Exceptions::ExportError.new("Incremental Export Error") if result.length > 0
      end
    end

    def dump_export metadata
      SqlCommands.mysqldump(:db => @export_obj.source_schema,
                            :filepath => filepath)    
    end
    
    ##
    # Executes export command via ssh on the source DB
    # Updates/interacts with the metadata object
    ##
    def execute_export cmd, metadata
      puts "IN EXPORT......."
      ssh = @export_obj.ssh_to_source
      metadata.ssh = ssh
      metadata.store!
      result = ""

      begin
        # Execute Export command on the source DB server
        result = ssh.exec!(cmd)
        
        # zip the output
        r = ssh.exec!(zipfile)
        puts r
      ensure
        metadata.state = "exported"
        metadata.zipped = true
      end

      return result
    end

    ##
    # zips the file on the source DB server
    ##
    def zipfile
      cmd = "cd #{Myreplicator.configs[@export_obj.source_schema]["ssh_tmp_dir"]}; gzip #{@export_obj.filename}"
      puts cmd
      return cmd
    end
    
  end
end
