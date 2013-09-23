module Myreplicator
  class Sweeper

    @queue = :myreplicator_sweeper # Provided for Resque
    ##
    # Main method provided for resque
    # Reconnection provided for resque workers
    ##
    def self.perform
      Myreplicator::Log.clear_deads
      Myreplicator::Log.clear_stucks
      Myreplicator::Log.clear_olds
      ActiveRecord::Base.configurations.keys.each do |db|
        Myreplicator::VerticaLoader.clean_up_temp_tables(db)
      end

      #removing files that are left in the storage for more than 12 hours
      folders = [
        "#{Myreplicator.tmp_path}",
        "#{Myreplicator.configs[Myreplicator.configs.keys[1]]["ssh_tmp_dir"]}"
      ]

      folders.each do |folder|
        cmd = "find #{folder} -mmin +720"
        l = `#{cmd}`
        list = l.split(/\n/)
        list.each do |file|
          file.chomp!
          if File.file?(file)
            if (file.split('.').last == 'gz') || (file.split('.').last == 'json')
              puts "=== #{file}"
              File.delete(file)
            end
          end
        end
      end

    end

  end
end