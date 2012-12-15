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

      # Database host when ssh'ed into the db server
      db_host = ssh_configs(db)["ssh_db_host"].nil? ? "127.0.0.1" : ssh_configs(db)["ssh_db_host"]

      cmd = Myreplicator.mysqldump
      cmd += "#{flags} -u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} "
      cmd += "-h#{db_host} " if db_configs(db)["host"]
      cmd += " -P#{db_configs(db)["port"]} " if db_configs(db)["port"]
      cmd += " #{db} "
      cmd += " #{options[:table_name]} "
      cmd += "--result-file=#{options[:filepath]} "

      # cmd += "--tab=#{options[:filepath]} "
      # cmd += "--fields-enclosed-by=\'\"\' "
      # cmd += "--fields-escaped-by=\'\\\\\' "

      return cmd
    end

    def self.db_configs db
      ActiveRecord::Base.configurations[db]
    end

    def self.ssh_configs db
      Myreplicator.configs[db]
    end

    def self.dump_flags
      {"add-locks" => true,
        "compact" => false,
        "lock-tables" => false,
        "no-create-db" => true,
        "no-data" => false,
        "quick" => true,
        "skip-add-drop-table" => false,
        "create-options" => false,
        "single-transaction" => false
      }
    end

    def self.mysql_export *args
      options = args.extract_options!
      options.reverse_merge! :flags => []
      db = options[:db]
      # Database host when ssh'ed into the db server
      db_host = ssh_configs(db)["ssh_db_host"].nil? ? "127.0.0.1" : ssh_configs(db)["ssh_db_host"]

      flags = ""

      self.mysql_flags.each_pair do |flag, value|
        if options[:flags].include? flag
          flags += " --#{flag} "
        elsif value
          flags += " --#{flag} "
        end
      end

      cmd = Myreplicator.mysql
      cmd += "#{flags} -u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} " 
      cmd += "-h#{db_host} " if db_configs(db)["host"].blank?
      cmd += db_configs(db)["port"].blank? ? "-P3306 " : "-P#{db_configs(db)["port"]} "
      cmd += "--execute=\"#{options[:sql]}\" "
      cmd += " > #{options[:filepath]} "
      
      puts cmd
      return cmd
    end

    def self.mysql_export_outfile *args
      options = args.extract_options!
      options.reverse_merge! :flags => []
      db = options[:db]
      Kernel.p options
      # Database host when ssh'ed into the db server
      db_host = ssh_configs(db)["ssh_db_host"].nil? ? "127.0.0.1" : ssh_configs(db)["ssh_db_host"]

      flags = ""

      self.mysql_flags.each_pair do |flag, value|
        if options[:flags].include? flag
          flags += " --#{flag} "
        elsif value
          flags += " --#{flag} "
        end
      end

      sql = "SELECT * INTO OUTFILE #{options[:filepath]} FROM #{options[:db]}.#{options[:table]}" 
      
      if options[:incremental_col] && options[:incremental_val]
        if options[:incremental_col_type] == "datetime"
          sql += "WHERE #{options[:incremental_col]} >= '#{options[:incremental_val]}'"
        else
          sql += "WHERE #{options[:incremental_col]} >= #{options[:incremental_val]}"
        end
      end

      sql += " FIELDS TERMINATED BY ';~;' OPTIONALLY ENCLOSED BY '\"'  LINES TERMINATED BY '\n'"
      
      puts sql

      cmd = Myreplicator.mysql
      cmd += "#{flags} -u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} " 
      cmd += "-h#{db_host} " if db_configs(db)["host"].blank?
      cmd += db_configs(db)["port"].blank? ? "-P3306 " : "-P#{db_configs(db)["port"]} "
      cmd += "--execute=\"#{options[:sql]}\" "
      
      puts cmd

      return cmd
    end

    def self.mysql_flags
      {"column-names" => false,
        "quick" => true,
        "reconnect" => true
      }    
    end

    def self.export_sql *args
      options = args.extract_options!
      sql = "SELECT * FROM #{options[:db]}.#{options[:table]} " 
      
      if options[:incremental_col] && options[:incremental_val]
        if options[:incremental_col_type] == "datetime"
          sql += "WHERE #{options[:incremental_col]} >= '#{options[:incremental_val]}'"
        else
          sql += "WHERE #{options[:incremental_col]} >= #{options[:incremental_val]}"
        end
      end

      return sql
    end

    def self.max_value_sql *args
      options = args.extract_options!
      sql = ""

      if options[:incremental_col]
        sql = "SELECT max(#{options[:incremental_col]}) FROM #{options[:db]}.#{options[:table]}" 
      else
        raise Myreplicator::Exceptions::MissingArgs.new("Missing Incremental Column Parameter")
      end
      
      return sql
    end

  end
end
