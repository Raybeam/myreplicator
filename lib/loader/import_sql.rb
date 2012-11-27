module Myreplicator
  class ImportSql
    class << self
      
      def load_data_infile *args
        options = args.extract_options!
        options.reverse_merge! :replace => false

        sql = build_load_data_infile options

        puts sql
        cmd = mysql_cmd
        cmd += "-e #{sql} "

        puts cmd

        return cmd
      end
      
      def build_load_data_infile options
        handle = options[:replace] ? 'REPLACE' : 'IGNORE' 
        
        sql = "LOAD DATA LOCAL INFILE '#{options[:filename]}' #{handle} INTO TABLE #{options[:table_name]} "
        
        if options.include?(:character_set)
          sql << " CHARACTER SET #{options[:character_set]}"
        end
        
        if options.include?(:fields_terminated_by) or options.include?(:enclosed_by) or options.include?(:escaped_by)
          sql << " FIELDS"
        end
        if options.include?(:fields_terminated_by)
          sql << " TERMINATED BY #{quote(options[:fields_terminated_by])}"
        end
        if options.include?(:enclosed_by)
          sql << " ENCLOSED BY #{quote(options[:enclosed_by])}"
        end
        if options.include?(:escaped_by)
          sql << " ESCAPED BY #{quote(options[:escaped_by])}"
        end
        
        if options.include?(:starting_by) or options.include?(:lines_terminated_by)
          sql << " LINES"
        end
        if options.include?(:starting_by) 
          sql << " STARTING BY #{quote(options[:starting_by])}"
        end
        if options.include?(:lines_terminated_by)
          sql << " TERMINATED BY #{quote(options[:lines_terminated_by])}"
        end

        if options.include?(:ignore)
          sql << " IGNORE #{options[:ignore]} LINES"
        end
        
        if options.include?(:fields) and !options[:fields].empty?
          sql << "( #{options[:fields].join(', ')} )"
        end

        return sql
      end

      def initial_load *args
        options = args.extract_options!
        db = options[:db]
        cmd = mysql_cmd
        puts cmd
        
        cmd += " #{db} "
        cmd += " #{options[:table_name]} "
        cmd += " < #{options[:filepath]} "
        
        puts cmd
        return cmd
      end
      
      def mysql_cmd
        # Destination database host
        db_host = db_configs(db).has_key?("host") ? db_configs(db)["host"] : "127.0.0.1"
        
        cmd = Myreplicator.mysql
        cmd += "-u#{db_configs(db)["username"]} -p#{db_configs(db)["password"]} "
        cmd += "-h#{db_host} " 
        cmd += " -P#{db_configs(db)["port"]} " if db_configs(db)["port"]
        
        return cmd
      end
      
    end #self

  end
end
