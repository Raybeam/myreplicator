module Myreplicator
  module SqlCommands
    
    def self.mysqldump *args
      options = args.extract_options!
      options.reverse_merge! :flags => []
      db = options[:db]

      flags = ""

      self.dump_flags.each_pair do |flag, value|
        if options[:flags].include? flag
          flags += " --#{flag} "
        elsif value
          flags += " --#{flag} "
        end
      end

      cmd = "mysqldump #{flags} -u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} " 
      cmd += "-h#{db_configs(db)["host"]} -P#{db_configs(db)["port"]} "
      cmd += " result-file=#{options[:filepath]} "

      puts cmd
      return cmd
    end

    def self.db_configs db
      ActiveRecord::Base.configurations[myreplicator][db]
    end
    
    def self.dump_flags
      {"add-locks" => true,
        "compact" => false,
        "lock-tables" => false,
        "no-create-db" => true,
        "no-data" => false,
        "quick" => true,
        "skip-add-drop-table" => true,
        "create-options" => false,
        "single-transaction" => false
      }
    end
    
  end
end
