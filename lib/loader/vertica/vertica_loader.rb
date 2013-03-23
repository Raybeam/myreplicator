module Myreplicator
  class VerticaLoader
    class << self

      def create_table *args
        options = args.extract_options!
        Kernel.p "===== OPTION ====="
              puts options
        columns = []
        options[:mysql_schema].each(:as => :hash) do |row|
          columns << row
        end
        options[:columns] = columns

        sql = Myreplicator::VerticaSql.create_table_stmt options
        puts sql
        VerticaDb::Base.connection.execute sql
      end

      def destination_table_vertica options
        sql = "select column_name, data_type From columns where 
                 table_name = '#{options[:table]}' AND table_schema = '#{options[:destination_schema]}'"
        puts sql
        result = DB.exec_sql("vertica",sql)
        return result     
      end

      ##
      # Schema Change Algorithm
      # Create temp table to load all data
      # Load data 
      # Drop table
      # Rename table
      ##
      # rasing a concern: using the same schema or the tmp schema for the tmp table? Vertica doesn't lock the schema
      def apply_schema_change options, temp_table
        Kernel.p "+++++++++++++++++ options "
        puts options
        VerticaLoader.create_table({:mysql_schema => options[:mysql_schema],
                                     :vertica_db => options[:vertica_db], 
                                     :vertica_schema => options[:vertica_schema],
                                     :table => temp_table,
                                     :mysql_table => options[:table]})
        table = options[:table]
        export_id = options[:export_id]
        new_options = prepare_options options
        new_options[:file] = options[:filepath]
        new_options[:table] = temp_table
        new_options[:schema] = options[:vertica_schema]
        
         
        vertica_copy new_options
        
        Kernel.p "+++++++++++++++++ new_options "
        puts new_options
        options[:table] = table
        puts options
        # drop the old table
        sql = "DROP TABLE IF EXISTS #{options[:vertica_db]}.#{options[:vertica_schema]}.#{options[:table]} CASCADE;"
        VerticaDb::Base.connection.execute sql
        # rename
        sql = "ALTER TABLE #{options[:vertica_db]}.#{options[:vertica_schema]}.#{temp_table} RENAME TO #{options[:table]};"
        VerticaDb::Base.connection.execute sql

      end
      
      def create_temp_table *args
        options = args.extract_options!
        temp_table_name = "temp_" + options[:table] + DateTime.now.strftime('%Y%m%d_%H%M%S').to_s

        VerticaLoader.create_table({:mysql_schema => options[:mysql_schema],
                                     :vertica_db => options[:vertica_db], 
                                     :vertica_schema => options[:vertica_schema],
                                     :table => temp_table_name,
                                     :mysql_table => options[:table]})
        return temp_table_name
      end
   
      def prepare_options *args
        #options = args.extract_options!.clone
        options = args.extract_options!
        Kernel.p "===== OPTION  [options[:db]] ====="
        puts options
        # How not to hard code the vertica connection config ?
        vertica_options = ActiveRecord::Base.configurations["vertica"]

        options.reverse_merge!(:host => vertica_options["host"],
                              :user => vertica_options["username"],
                              :pass => vertica_options["password"],
                              :db   => vertica_options["database"],
                              :schema => options[:destination_schema],
                              :table => options[:table_name],
                              :file => options[:filepath],
                              :delimiter => "\\0",
                              :null_value => "NULL",
                              :enclosed => "")
      # working now but should fix this 
        if !vertica_options["vsql"].blank?
          options.reverse_merge!(:vsql => vertica_options["vsql"])
        else
          options.reverse_merge!(:vsql => "/opt/vertica/bin/vsql")
        end
        
        return options  
      end
      
      # Loader::VerticaLoader.load({:schema => "king", :table => "category_overview_data", :file => "tmp/vertica/category_overview_data.tsv", :null_value => "NULL"})
      # check for export_type!
      def load *args
        options = args.extract_options!
        metadata = options[:metadata]
        Kernel.p "===== metadata ====="
        Kernel.p metadata
        Kernel.p options
        #options = {:table => "app_csvs", :destination_schema => "public", :source_schema => "okl_dev"}
        #options = {:table => "actucast_appeal", :destination_schema => "public", :source_schema => "raw_sources"}
        schema_check = Myreplicator::MysqlExporter.schema_changed?(:table => options[:table_name], 
                                                     :destination_schema => options[:destination_schema], 
                                                     :source_schema => options[:source_schema])
        Kernel.p "===== schema_check ====="
        Kernel.p schema_check
        Kernel.p schema_check[:mysql_schema]
        #create a temp table
        temp_table = "temp_" + options[:table_name] + DateTime.now.strftime('%Y%m%d_%H%M%S').to_s
        ops = {:mysql_schema => schema_check[:mysql_schema],
          :vertica_db => options[:db],
          :vertica_schema => options[:destination_schema],
          :table => options[:table_name],
          :export_id => options[:export_id],
          :filepath => options[:filepath]
        }
        Kernel.p "===== schema_check[:mysql_schema] ====="
        Kernel.p ops
        if schema_check[:new]
          create_table(ops)
          #LOAD DATA IN
          vertica_copy options
        elsif schema_check[:changed]
          if metadata.export_type == 'initial'
            Kernel.p "===== schema_check[:changed] ====="
            Loader.clear_older_files metadata  # clear old incremental files
            apply_schema_change(ops, temp_table)
          else
            Loader.cleanup metadata #Remove incremental file
            Kernel.p "===== Remove incremental file ====="
          end
        else
          temp_table = create_temp_table ops
          options[:table] = temp_table
          Kernel.p "===== COPY TO TEMP TABLE #{temp_table} ====="
          vertica_copy options
          options.reverse_merge!(:temp_table => "#{temp_table}")
          options[:table] = options[:table_name]
          Kernel.p "===== MERGE ====="
          vertica_merge options
          drop the temp table
          Kernel.p "===== DROP TEMP TABLE ====="
          sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{temp_table} CASCADE;"
          VerticaDb::Base.connection.execute sql
        end
      end
      
      def vertica_copy * args
        options = args.extract_options!
        list_of_nulls =  ["0000-00-00"]
        prepared_options = prepare_options options
        if prepared_options[:file].blank?
          raise "No input file"
        end
        
        process_file(:file => prepared_options[:file], 
                     :list_of_nulls => list_of_nulls,
                     :null_value => prepared_options[:null_value])

        cmd = get_vsql_copy_command(prepared_options)
        puts cmd
        system(cmd)
      end
        
      def get_vsql_copy_command prepared_options
        file_extension = prepared_options[:file].split('.').last
        file_handler = ""
        file_handler = "GZIP" if file_extension == "gz" 
        sql = "COPY #{prepared_options[:schema]}.#{prepared_options[:table]} FROM LOCAL \'#{prepared_options[:file]}\' #{file_handler} DELIMITER E\'#{prepared_options[:delimiter]}\' NULL as \'#{prepared_options[:null_value]}\' ENCLOSED BY \'#{prepared_options[:enclosed]}\' EXCEPTIONS 'load_exceptions.log';"
        cmd = "#{prepared_options[:vsql]} -h #{prepared_options[:host]} -U #{prepared_options[:user]} -w #{prepared_options[:pass]} -d #{prepared_options[:db]} -c \"#{sql}\""
        return cmd
      end
      
      def process_file *args
        ### replace the null values in the input file 
        options = args.extract_options!
        options[:file].blank? ? return : file = options[:file]
        options[:list_of_nulls].blank? ? list_of_nulls = [] : list_of_nulls = options[:list_of_nulls]
        options[:null_value].blank? ? null_value = "NULL" : null_value = options[:null_value]
        Kernel.p "===== file #{file}====="
        file_extension = file.split('.').last
        Kernel.p "===== file_extension #{file_extension}====="
        
        case file_extension
        when "tsv", "csv"
          process_flat_file(file, list_of_nulls, null_value)
        when "gz"
          process_gzip_file(file, list_of_nulls, null_value)
        else
          raise "Un supported file extension"
        end
      end
      
      def replace_null(file, list_of_nulls, null_value = "NULL")
        list_of_nulls.each do | value|
          # special case for NULL MySQL datetime/date type but the column is defined NOT NULL
          extension = file.split('.').last
          if value == '0000-00-00'
            cmd1 = "sed -i 's/#{value}/1900-01-01/g' #{file}"
            Kernel.p cmd1
            system(cmd1)
          else
            cmd1 = "sed -i 's/#{value}/#{null_value}/g' #{file}"
            Kernel.p cmd1
            system(cmd1)
          end
        end
      end
      
      def process_flat_file file, list_of_nulls, null_value 
        # sed
        replace_null(file, list_of_nulls, null_value)
      end
      
      def process_gzip_file file, list_of_nulls, null_value
        # unzip
        temp_file = "tmp/temp_#{file.split('.').first.split('/').last}.txt"
        
        cmd = "gunzip -f #{file} -c > #{temp_file}"
        Kernel.p cmd
        system(cmd)
        # sed
        replace_null("#{temp_file}", list_of_nulls, null_value)
        # zip
        cmd2 = "gzip #{temp_file} -c > #{file}"
        Kernel.p cmd2
        system(cmd2)
        cmd3 = "rm #{temp_file}"
        Kernel.p cmd3
        system(cmd3)
      end

      def get_mysql_keys mysql_schema_simple_form
        result = []
        mysql_schema_simple_form.each do |col|
          if col["column_key"] == "PRI"
            result << col["column_name"]
          end
        end
        return result
      end
      
      def get_mysql_none_keys mysql_schema_simple_form
        result = []
        mysql_schema_simple_form.each do |col|
          if col["column_key"].blank?
            result << col["column_name"]
          end
        end
        return result
      end
      
      def get_mysql_inserted_columns mysql_schema_simple_form
        result = []
        mysql_schema_simple_form.each do |col|
          result << col["column_name"]
        end
        return result
      end
      
      def get_vsql_merge_command options, keys, none_keys, inserted_columns
        Kernel.p "===== Merge Options ====="
        Kernel.p options
        a = prepare_options options
        Kernel.p a
        prepared_options = options
        sql = "MERGE INTO "
        sql+= "#{prepared_options[:db]}.#{prepared_options[:schema]}.#{prepared_options[:table]} target "
        sql+= "USING #{prepared_options[:db]}.#{prepared_options[:schema]}.#{prepared_options[:temp_table]} source "
        sql+= "ON "
        count = 0
        keys.each do |k|
          if count < 1 
            sql += "source.#{k} = target.#{k} "
          else
            sql += "AND source.#{k} = target.#{k} "
          end
          count += 1
        end
        sql+= "WHEN MATCHED THEN "
        sql+= "UPDATE SET "
        count = 1
        none_keys.each do |nk|
          if count < none_keys.size
            sql+= "#{nk} = source.#{nk}, "
          else
            sql+= "#{nk} = source.#{nk} "
          end
          count += 1
        end
        sql+= "WHEN NOT MATCHED THEN "
        sql+= "INSERT "
        #count = 1
        #inserted_columns.each do |col|
        #  if count < inserted_columns.size
        #    sql+= "#{col}, "
        #  else
        #    sql+= "#{col} "
        #  end
        #  count += 1
        #end
        count = 1
        sql+= " VALUES ("
        inserted_columns.each do |col|
          if count < inserted_columns.size
            sql+= "source.#{col}, "
          else
            sql+= "source.#{col}) "
          end
          count += 1
        end  
        sql+= "; COMMIT;"  
        cmd = "#{prepared_options[:vsql]} -h #{prepared_options[:host]} -U #{prepared_options[:user]} -w #{prepared_options[:pass]} -d #{prepared_options[:db]} -c \"#{sql}\""
        return cmd    
      end
        
      def vertica_merge *args
        options = args.extract_options!
        metadata = options[:metadata]
        Kernel.p "===== MERGE metadata ====="
        Kernel.p metadata
        ops = {:table => options[:table_name], 
        :destination_schema => options[:destination_schema], 
        :source_schema => options[:source_schema]}
        mysql_schema = Loader.mysql_table_definition(options)
        vertica_schema = VerticaLoader.destination_table_vertica(options)
        mysql_schema_simple_form = MysqlExporter.get_mysql_schema_rows mysql_schema
        # get the column(s) that is(are) used as the primary key
        keys = get_mysql_keys mysql_schema_simple_form
        # get the non key coluns 
        none_keys = get_mysql_none_keys mysql_schema_simple_form
        # get the column to put in the insert part
        inserted_columns = get_mysql_inserted_columns mysql_schema_simple_form
        #get the vsql merge command 
        cmd = get_vsql_merge_command options, keys, none_keys, inserted_columns 
        #execute    
        puts cmd
        begin
          result = `#{cmd} 2>&1`
          if result[0..4] == "ERROR"
            Loader.cleanup metadata
            sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{options[:temp_table]} CASCADE;"
            Kernel.p "===== DROP CMD ====="
            Kernel.p sql
            VerticaDb::Base.connection.execute sql
            raise result
          end
        rescue Exception => e
          raise e.message 
        ensure
          # place holder
        end
      end
=begin
       def create_all_tables db
         tables = Myreplicator::DB.get_tables(db)
         sqls = {}
         tables.each do |table|
           puts "Creating #{db}.#{table}"
           sql = "DROP TABLE IF EXISTS #{db}.#{table} CASCADE;"
           VerticaDb::Base.connection.execute sql
           sql = Loader::VerticaLoader.create_table(:vertica_db => "bidw",
           :vertica_table => table,
           :vertica_schema => db,
           :table => table,
           :db => db)
           sqls["#{table}"] = sql
           VerticaDb::Base.connection.execute sql
         end
       end
=end
    end
  end
end

