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

      unique_jobs.each do |export|
        download export
      end
    end

    ##
    # Connect to server via SSH
    # Kicks off parallel download
    ##
    def self.download export
      ssh = export.ssh_to_source     
      parallel_download(export, ssh, completed_files(ssh, export))
    end

    ##
    # Gathers all files that need to be downloaded
    # Gives the queue to parallelizer library to download in parallel
    ##
    def self.parallel_download export, ssh, files    
      p = Parallelizer.new(:klass => "Myreplicator::Transporter")

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
    def self.download_file    
      proc = Proc.new { |params|
        ssh = params[0]
        export = params[1] 
        filename = params[2]

        Log.run(:job_type => "transporter", :name => "metadata_file", 
                :file => filename, :export_id => export.id) do |log|

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
              sftp.download!(dump_file, File.join(tmp_dir, dump_file.split("/").last))
              Transporter.remove!(ssh, json_file, dump_file)
            end
          elsif Transporter.junk_file?(metadata)
            Transporter.remove!(ssh, json_file, dump_file)
          end #if

        end
      }
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

    def self.remove! ssh, json_file, dump_file
      ssh.exec!("rm #{json_file}")
      ssh.exec!("rm #{dump_file}")           
    end

    ##
    # Gets all files ready to be exported from server
    ##
    def self.completed_files ssh, export
      done_files = ssh.exec!(get_done_files(export))

      unless done_files.blank?
        return done_files.split("\n")
      end

      return []
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
      cmd = "cd #{Myreplicator.configs[export.source_schema]["ssh_tmp_dir"]}; grep -ls export_completed *.json"
    end
    
  end
end
