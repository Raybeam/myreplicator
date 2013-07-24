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
      ActiveRecord::Base.configurations.keys.each do |db|
        Myreplicator::VerticaLoader.clean_up_temp_tables(db)
      end
      folders = ["/home/share/datareplicator",
        "/home/share/okl/bi_apps/datareplicator/mysqldumps"
      ]
        
      folders.each do |folder|
        #cmd = "find #{folder} -mtime +2"
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