require "exporter"

module Myreplicator
  class Transporter
    
    def initialize *args
      options = args.extract_options!
      @tmp_dir = File.join(Myreplicator.app_root,"tmp", "myreplicator")
      Dir.mkdir(@tmp_dir) unless File.directory?(@tmp_dir)
    end
    
    def transfer
      unique_jobs = Export.where("state != 'failed' and active = 1").group("source_schema")
      
      unique_jobs.each do |export|
        download export
      end
    end

    def download export
      ssh = export.ssh_to_source
      done_files = ssh.exec!(get_done_files)
      Kernel.p done_files
      unless done_files.blank?
        done_files.split("\n").each do |file|
          puts file
        end
      end
    end

    def get_done_files
      cmd = "cd #{Myreplicator.configs[self.source_schema]["ssh_tmp_dir"]}; grep -l export_completed *.json"
    end

    
  end
end
