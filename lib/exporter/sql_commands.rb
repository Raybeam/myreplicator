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
      db_host = "127.0.0.1" 

      if !ssh_configs(db)["ssh_db_host"].blank? 
        db_host =  ssh_configs(db)["ssh_db_host"]
      elsif !db_configs(db)["host"].blank?
        db_host = db_configs(db)["host"]
      end

      cmd = Myreplicator.mysqldump
      cmd += "#{flags} -u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} "
      cmd += "-h#{db_host} "
      cmd += " -P#{db_configs(db)["port"]} " if db_configs(db)["port"]
      cmd += " #{db} "
      cmd += " #{options[:table_name]} "
      cmd += "--result-file=#{options[:filepath]} "

      # cmd += "--tab=#{options[:filepath]} "
      # cmd += "--fields-enclosed-by=\'\"\' "
      # cmd += "--fields-escaped-by=\'\\\\\' "
        
      return cmd
    end

    ##
    # Db configs for active record connection
    ## 

    def self.db_configs db
      ActiveRecord::Base.configurations[db]
    end

    ##
    # Configs needed for SSH connection to source server
    ##

    def self.ssh_configs db
      Myreplicator.configs[db]
    end

    ##
    # Default dump flags
    ## 
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

    ##
    # Mysql exports using -e flag
    ## 

    def self.mysql_export *args
      options = args.extract_options!
      options.reverse_merge! :flags => []
      db = options[:db]
      
      # Database host when ssh'ed into the db server
      
      db_host = "127.0.0.1" 
      
      if !ssh_configs(db)["ssh_db_host"].blank? 
        db_host =  ssh_configs(db)["ssh_db_host"]
      elsif !db_configs(db)["host"].blank?
        db_host = db_configs(db)["host"]
      end
      
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

      cmd += "-h#{db_host} " 
      cmd += db_configs(db)["port"].blank? ? "-P3306 " : "-P#{db_configs(db)["port"]} "
      cmd += "--execute=\"#{options[:sql]}\" "
      cmd += " > #{options[:filepath]} "
      
      puts cmd
      return cmd
    end

    ##
    # Mysql export data into outfile option
    # Provided for tables that need special delimiters
    ##
    
    def self.get_outfile_sql options 
      Kernel.p "===== SELECT * INTO OUTFILE OPTIONS====="
      Kernel.p options
      sql = "SELECT * INTO OUTFILE '#{options[:filepath]}' " 
      
      sql += " FIELDS TERMINATED BY '\\0' ESCAPED BY '' OPTIONALLY ENCLOSED BY '\\\"'  LINES TERMINATED BY '\\n'"
      
      sql += "FROM #{options[:db]}.#{options[:table]} "

      if !options[:incremental_col].blank? && !options[:incremental_val].blank?
        if options[:incremental_col_type] == "datetime"
          sql += "WHERE #{options[:incremental_col]} >= '#{options[:incremental_val]}'"
        else
          sql += "WHERE #{options[:incremental_col]} >= #{options[:incremental_val]}"
        end
      end

      return sql
    end

    ##
    # Export using outfile
    # \\0 delimited
    # terminated by newline 
    # Location of the output file needs to have 777 perms
    ##
    def self.mysql_export_outfile *args
      Kernel.p "===== mysql_export_outfile OPTIONS ====="
       
      options = args.extract_options!
      Kernel.p options
      options.reverse_merge! :flags => []
      db = options[:source_schema]

      # Database host when ssh'ed into the db server
      db_host = "127.0.0.1"
      Kernel.p "===== mysql_export_outfile ssh_configs ====="
      Kernel.p ssh_configs(db)
      if !ssh_configs(db)["ssh_db_host"].blank?
        db_host =  ssh_configs(db)["ssh_db_host"]
      elsif !db_configs(db)["host"].blank?
        db_host = db_configs(db)["host"]
      end
      
      flags = ""
      
      self.mysql_flags.each_pair do |flag, value|
        if options[:flags].include? flag
          flags += " --#{flag} "
        elsif value
          flags += " --#{flag} "
        end
      end

      cmd = Myreplicator.mysql
      cmd += "#{flags} "
      
      if db_configs(db).has_key? "socket"
        cmd += "--socket=#{db_configs(db)["socket"]} " 
      else
        cmd += "-u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} " 
      end
      
      cmd += "-h#{db_host} " 
      cmd += db_configs(db)["port"].blank? ? "-P3306 " : "-P#{db_configs(db)["port"]} "
      cmd += "--execute=\"#{get_outfile_sql(options)}\" "
      
      puts cmd
      return cmd
    end

    ##
    # Default flags for mysql export
    ## 
    def self.mysql_flags
      {"column-names" => false,
        "quick" => true,
        "reconnect" => true
      }    
    end

    ##
    # Builds SQL needed for incremental exports
    ##
    def self.export_sql *args
      options = args.extract_options!
      sql = "SELECT * FROM #{options[:db]}.#{options[:table]} " 
      
      if options[:incremental_col] && !options[:incremental_val].blank?
        if options[:incremental_col_type] == "datetime"
          sql += "WHERE #{options[:incremental_col]} >= '#{options[:incremental_val]}'"
        else
          sql += "WHERE #{options[:incremental_col]} >= #{options[:incremental_val]}"
        end
      end

      return sql
    end

    ##
    # Gets the Maximum value for the incremental 
    # column of the export job
    ##
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
