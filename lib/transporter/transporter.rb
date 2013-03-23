require "exporter"

module Myreplicator
  class Transporter

    @queue = :myreplicator_transporter # Provided for Resque

    def initialize *args
      options = args.extract_options!
    end

    def tmp_dir
      @tmp_dir ||= File.join(Myreplicator.app_root,"tmp", "myreplicator")
      Dir.mkdir(@tmp_dir) unless File.directory?(@tmp_dir)
      @tmp_dir
    end

    ##
    # Main method provided for resque
    # Reconnection provided for resque workers
    ##
    def self.perform
      transfer # Kick off the load process
    end

    ##
    # Schedules the transport job in Resque
    ##
    def schedule cron
      Resque.set_schedule("myreplicator_transporter", {
                            :cron => cron,
                            :class => "Myreplicator::Transporter",
                            :queue => "myreplicator_transporter"
                          })
    end
    
    ##
    # Connects to all unique database servers 
    # downloads export files concurrently from multiple sources
    ##
    def self.transfer
      unique_jobs = Export.where("active = 1").group("source_schema")
      Kernel.p "===== unique_jobs ====="
      Kernel.p unique_jobs 
      unique_jobs.each do |export|
        download export
      end
    end

    ##
    # Connect to server via SSH
    # Kicks off parallel download
    ##
    def self.download export
      Kernel.p "===== 1 ====="
      parallel_download(completed_files(export))
    end

    ##
    # Gathers all files that need to be downloaded
    # Gives the queue to parallelizer library to download in parallel
    ##
    def self.parallel_download files    
      p = Parallelizer.new(:klass => "Myreplicator::Transporter")
    
      files.each do |f|
        puts f[:file]
        p.queue << {:params =>[f[:export], f[:file]], :block => download_file}
      end

      p.run 
    end

    ##
    # Code block that each thread calls
    # instance_exec is used to execute under Transporter class
    # 1. Connects via SFTP
    # 2. Downloads metadata file first
    # 3. Gets dump file location from metadata
    # 4. Downloads dump file
    ##
    def self.download_file    
      proc = Proc.new { |params|
        export = params[0] 
        filename = params[1]

        ActiveRecord::Base.verify_active_connections!
        ActiveRecord::Base.connection.reconnect!

        Log.run(:job_type => "transporter", :name => "metadata_file", 
                :file => filename, :export_id => export.id ) do |log|

          sftp = export.sftp_to_source
          json_file = Transporter.remote_path(export, filename) 
          json_local_path = File.join(tmp_dir,filename)
          puts "Downloading #{json_file}"
          sftp.download!(json_file, json_local_path)
          metadata = Transporter.metadata_obj(json_local_path)
          dump_file = metadata.export_path
          puts metadata.state
          if metadata.state == "export_completed"
            Log.run(:job_type => "transporter", :name => "export_file",
                    :file => dump_file, :export_id => export.id) do |log|
              puts "Downloading #{dump_file}"
              local_dump_file = File.join(tmp_dir, dump_file.split("/").last)
              sftp.download!(dump_file, local_dump_file)
              Transporter.remove!(export, json_file, dump_file)
              #export.update_attributes!({:state => 'transport_completed'})
              # store back up as well
              unless metadata.store_in.blank?
                Transporter.backup_files(metadata.backup_path, json_local_path, local_dump_file)
              end
            end
          end #if
          puts "#{Thread.current.to_s}___Exiting download..."
        end
      }
    end

    def self.backup_files location, metadata_path, dump_path
      FileUtils.cp(metadata_path, location)
      FileUtils.cp(dump_path, location)
    end

    ##
    # Returns true if the file should be deleted
    ##
    def self.junk_file? metadata
      case metadata.state
      when "failed"
        return true
      when "ignored"
        return true
      end
      return false
    end

    def self.remove! export, json_file, dump_file
      ssh = export.ssh_to_source
      puts "rm #{json_file} #{dump_file}"
      ssh.exec!("rm #{json_file} #{dump_file}")
    end

    ##
    # Gets all files ready to be exported from server
    ##
    def self.completed_files export
      ssh = export.ssh_to_source
      done_files = ssh.exec!(get_done_files(export))
      if done_files.blank?
        return []
      end
      files = done_files.split("\n")
      
      jobs = Export.where("active = 1 and source_schema = '#{export.source_schema}'")
      #jobs.each do |j|
      #  j.update_attributes!({:state => "transporting"})
      #end
      result = []
      files.each do |file|
        flag = nil
        jobs.each do |job|
          if file.include?(job.table_name)
            flag = job 
            #job.update_attributes!({:state => 'transporting'})
          end
        end
        if flag
          result << {:file => file, :export => flag}
        end
      end
      Kernel.p "===== done_files ====="
      Kernel.p result
      return result

      #Kernel.p "===== done_files ====="
      #Kernel.p files
      #return files
    end

    def self.metadata_obj json_path
      metadata = ExportMetadata.new(:metadata_path => json_path)
      return metadata
    end

    ##
    # Reads metadata file for the export path
    ##
    def self.get_dump_path json_path, metadata = nil
      metadata = Transporter.metadata_obj(json_path) if metadata.nil?
      return metadata.export_path
    end

    ##
    # Returns where path of dump files on remote server 
    ## 
    def self.remote_path export, filename
      File.join(Myreplicator.configs[export.source_schema]["ssh_tmp_dir"], filename)
    end

    ##
    # Command for list of done files
    # Grep -s used to supress error messages
    ## 
    def self.get_done_files export
      Kernel.p "===== export ====="
      Kernel.p export
      cmd = "cd #{Myreplicator.configs[export.source_schema]["ssh_tmp_dir"]}; grep -ls export_completed *.json"
    end
    
  end
end
