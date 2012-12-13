require "exporter"

module Myreplicator
  class Transporter
    
    def initialize *args
      options = args.extract_options!
    end

    def tmp_dir
      @tmp_dir ||= File.join(Myreplicator.app_root,"tmp", "myreplicator")
      Dir.mkdir(@tmp_dir) unless File.directory?(@tmp_dir)
      @tmp_dir
    end
    
    ##
    # Connects to all unique database servers 
    # downloads export files concurrently from multiple sources
    # TO DO: Clean up after transfer job is done
    ##
    def transfer
      unique_jobs = Export.where("state != 'failed' and active = 1").group("source_schema")
      
      unique_jobs.each do |export|
        download export
      end
    end

    ##
    # Connect to server via SSH
    # Kicks of parallel download
    ##
    def download export
      ssh = export.ssh_to_source     
      parallel_download(export, ssh, completed_files(ssh, export))
    end

    ##
    # Gathers all files that need to be downloaded
    # Gives the queue to parallelizer library to download in parallel
    ##
    def parallel_download export, ssh, files    
      p = Parallelizer.new
      
      files.each do |filename|
        puts filename
        p.queue << {:params =>[ssh, export, filename], :block => download_file}
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
    def download_file    
      proc = Proc.new { |params|
        ssh = params[0]
        export = params[1] 
        filename = params[2]
        Log.run(:job_type => "transporter", :name => "metadata_file", :file => filename, :export_id => export.id) do |log|
          sftp = export.sftp_to_source
          json_file = remote_path(export, filename) 
          json_local_path = File.join(tmp_dir,filename)
          puts "Downloading #{json_file}"
          sftp.download!(json_file, json_local_path)
          dump_file = get_dump_path(json_local_path)
          Log.run(:job_type => "transporter", :name => "export_file", :file => dump_file, :export_id => export.id) do |log|
            puts "Downloading #{dump_file}"
            sftp.download!(dump_file, File.join(tmp_dir, dump_file.split("/").last))             
          end
        end
      }
    end

    ##
    # Gets all files ready to be exported from server
    ##
    def completed_files ssh, export
      done_files = ssh.exec!(get_done_files(export))

      unless done_files.blank?
        return done_files.split("\n")
      end

      return []
    end

    ##
    # Reads metadata file for the export path
    ##
    def get_dump_path json_path
      metadata = ExportMetadata.new(:metadata_path => json_path)
      return metadata.export_path
    end

    ##
    # Returns where path of dump files on remote server 
    ## 
    def remote_path export, filename
      File.join(Myreplicator.configs[export.source_schema]["ssh_tmp_dir"], filename)
    end

    ##
    # Command for list of done files
    ## 
    def get_done_files export
      cmd = "cd #{Myreplicator.configs[export.source_schema]["ssh_tmp_dir"]}; grep -l export_completed *.json"
    end
    
  end
end
