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

        metadata.on_failure do |m|
          update_export(:state => "failed", :export_finished_at => Time.now, :error => metadata.error)
        end

        prepare metadata

        if @export_obj.state == "new"
          update_export(:state => "running", :exporter_pid => Process.pid)
          initial_export metadata
          wrapup metadata
        elsif !is_running?
          update_export(:state => "running", :exporter_pid => Process.pid)
          max_value = incremental_export(metadata)
          metadata.on_success do |m|
            update_export(:state => "failed", :export_finished_at => Time.now, :error => metadata.error)
          end
          wrapup metadata
        end
        
      end
    end
    
    def prepare metadata
      ssh = @export_obj.ssh_to_source
      metadata.ssh = ssh
    end
    
    def is_running?
      return false if @export_obj.state != "running"
      begin
        Process.getpgid(@export_obj.exporter_pid)
        puts "IS RUNNING"
        return true
      rescue Errno::ESRCH
        puts "IS NOT RUNNING"
        return false
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
        
      update_export(:state => "exporting", :export_started_at => Time.now)
      result = execute_export(cmd, metadata)

      check_result(result, 0)
    end

    ##
    # Exports table incrementally, using the incremental column specified
    # If column is not specified, it will export the entire table 
    # Maximum value of the incremental column is recorded BEFORE export starts
    ##

    def incremental_export metadata
      max_value = @export_obj.max_value
      @export_obj.update_max_val if @export_obj.max_incremental_value.blank?   

      sql = SqlCommands.export_sql(:db => @export_obj.source_schema,
                                   :table => @export_obj.table_name,
                                   :incremental_col => @export_obj.incremental_column,
                                   :incremental_col_type => @export_obj.incremental_column_type,
                                   :incremental_val => @export_obj.max_incremental_value)      

      cmd = SqlCommands.mysql_export(:db => @export_obj.source_schema,
                                     :filepath => filepath,
                                     :sql => sql)
      
      update_export(:state => "exporting", :export_started_at => Time.now)  
      result = execute_export(cmd, metadata)
      check_result(result, 0)
    end


    ##
    # Completes an export process
    # Zips files, updates states etc
    ##
    def wrapup metadata
      puts "Zipping..."
      zip_result = metadata.ssh.exec!(zipfile)
      puts zip_result
      update_export(:state => "export_completed", :export_finished_at => Time.now)
      puts "Done.."
    end

    ##
    # Checks the returned resut from SSH CMD
    # Size specifies if there should be any returned results or not
    ##
    def check_result result, size
      unless result.nil?
        raise Exceptions::ExportError.new("Export Error\n#{result}") if result.length > 0
      end     
    end

    ##
    # Executes export command via ssh on the source DB
    # Updates/interacts with the metadata object
    ##
    def execute_export cmd, metadata
      metadata.store!
      result = ""

      # Execute Export command on the source DB server
      result = metadata.ssh.exec!(cmd)
      
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
