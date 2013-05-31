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
        #VerticaDb::Base.connection.execute sql
        Myreplicator::DB.exec_sql("vertica",sql)
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
        #VerticaDb::Base.connection.execute sql
        Myreplicator::DB.exec_sql("vertica",sql)
        # rename
        sql = "ALTER TABLE #{options[:vertica_db]}.#{options[:vertica_schema]}.#{temp_table} RENAME TO \"#{options[:table]}\";"
        
        #VerticaDb::Base.connection.execute sql
        Myreplicator::DB.exec_sql("vertica",sql)
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
        enclosed_by = Myreplicator.configs[options[:source_schema]]['enclosed_by']
        options.reverse_merge!(:host => vertica_options["host"],
                              :user => vertica_options["username"],
                              :pass => vertica_options["password"],
                              :db   => vertica_options["database"],
                              :schema => options[:destination_schema],
                              :table => options[:table_name],
                              :file => options[:filepath],
                              :delimiter => "\\0",
                              :null_value => "NULL",
                              :line_terminate => ";~~;\n",
                              :enclosed => "#{Myreplicator.configs[options[:source_schema]]['enclosed_by']}")
                              #:enclosed => '\"')
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
        Kernel.p "===== metadata.export_type ====="
        Kernel.p metadata.export_type
        Kernel.p options
        #options = {:table_name => "actucast_appeal", :destination_schema => "public", :source_schema => "raw_sources"}
        schema_check = Myreplicator::MysqlExporter.schema_changed?(:table => options[:table_name], 
                                                     :destination_schema => options[:destination_schema], 
                                                     :source_schema => options[:source_schema])
        Kernel.p "===== schema_check ====="
        Kernel.p schema_check
        #create a temp table
        temp_table = "temp_" + options[:table_name] + DateTime.now.strftime('%Y%m%d_%H%M%S').to_s
        ops = {:mysql_schema => schema_check[:mysql_schema],
          :vertica_db => options[:db],
          :vertica_schema => options[:destination_schema],
          :source_schema => options[:source_schema],
          :table => options[:table_name],
          :export_id => options[:export_id],
          :filepath => options[:filepath]
        }
        exp = Myreplicator::Export.find(metadata.export_id)
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
        elsif exp.nightly_refresh && (exp.nightly_refresh_frequency != 0) && ((Time.now() - exp.nightly_refresh_last_run) >= exp.nightly_refresh_frequency.minute)          
          Loader.clear_older_files metadata  # clear old incremental files
          exp.nightly_refresh_last_run = Time.now().change(:min => 0)
          exp.save!
          sql = "TRUNCATE TABLE #{options[:db]}.#{options[:destination_schema]}.#{options[:table_name]};"
          Myreplicator::DB.exec_sql("vertica",sql)
          # run the export. The next time loader runs, it will load the file
          exp.export
        elsif get_analyze_constraints(ops) > 0 # check for primary key/unique keys violations
          Kernel.p "===== DROP CURRENT TABLE ====="
          sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{options[:table_name]} CASCADE;"
          Myreplicator::DB.exec_sql("vertica",sql)
          # run the export. The next time loader runs, it will load the file
          exp.export
        else # incremental load
          temp_table = create_temp_table ops
          options[:table] = temp_table
          Kernel.p "===== COPY TO TEMP TABLE #{temp_table} ====="
          vertica_copy options
          options.reverse_merge!(:temp_table => "#{temp_table}")
          options[:table] = options[:table_name]
          sql = "SELECT COUNT(*) FROM #{options[:db]}.#{options[:destination_schema]}.#{options[:temp_table]};"
          result = Myreplicator::DB.exec_sql("vertica",sql)
          #temporary fix for racing refresh cause by one worker doing loader for many export jobs. Better fix: each export job starts its own loader worker
          if result.entries.first[:COUNT] == 0
            Kernel.p "===== DROP TEMP TABLE ====="
            sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{temp_table} CASCADE;"
            Myreplicator::DB.exec_sql("vertica",sql)
          else
            if exp.export_type == 'all'
              Kernel.p "===== DROP CURRENT TABLE ====="
              sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{options[:table]} CASCADE;"
              Myreplicator::DB.exec_sql("vertica",sql)
              sql = "ALTER TABLE #{options[:db]}.#{options[:destination_schema]}.#{options[:temp_table]} RENAME TO \"#{options[:table]}\";"
              Kernel.p sql
              Myreplicator::DB.exec_sql("vertica",sql)
            elsif exp.export_type == 'incremental'
              Kernel.p "===== MERGE ====="
              vertica_merge options
              #drop the temp table
              Kernel.p "===== DROP TEMP TABLE ====="
              sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{temp_table} CASCADE;"
              Myreplicator::DB.exec_sql("vertica",sql)
            end
          end
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
        Kernel.p "===== get_vsql_copy_command prepared_options ====="
        Kernel.p prepared_options
        file_extension = prepared_options[:file].split('.').last
        file_handler = ""
        file_handler = "GZIP" if file_extension == "gz" 
        tmp_dir = Myreplicator.tmp_path
        sql = "COPY #{prepared_options[:schema]}.#{prepared_options[:table]} FROM LOCAL \'#{prepared_options[:file]}\' #{file_handler} DELIMITER E\'#{prepared_options[:delimiter]}\' NULL as \'#{prepared_options[:null_value]}\' ENCLOSED BY E\'#{prepared_options[:enclosed]}\' RECORD TERMINATOR \'#{prepared_options[:line_terminate]}\' EXCEPTIONS '#{tmp_dir}/load_logs/#{prepared_options[:schema]}_#{prepared_options[:table_name]}.log';"
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
        tmp_dir = Myreplicator.tmp_path
        temp_file = "#{tmp_dir}/temp_#{file.split('.').first.split('/').last}.txt"
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
          if col["column_key"] != "PRI"
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
            if result[0..9] == "ERROR 4115"
              # drop the current table. In the next run the table will be re-created and COPY is used to load data into it
              sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{options[:table]} CASCADE;"
              Kernel.p "===== DROP CMD ====="
              Kernel.p sql
              Myreplicator::DB.exec_sql("vertica",sql)
              sql = "ALTER TABLE #{options[:db]}.#{options[:destination_schema]}.#{options[:temp_table]} RENAME TO \"#{options[:table]}\";"
              Kernel.p sql
              Myreplicator::DB.exec_sql("vertica",sql)
            else
              Loader.cleanup metadata
              sql = "DROP TABLE IF EXISTS #{options[:db]}.#{options[:destination_schema]}.#{options[:temp_table]} CASCADE;"
              Kernel.p "===== DROP CMD ====="
              Kernel.p sql
              #VerticaDb::Base.connection.execute sql
              Myreplicator::DB.exec_sql("vertica",sql)
              raise result
            end
          end
        rescue Exception => e
          raise e.message 
        ensure
          # place holder
        end
      end
      
      def  clean_up_temp_tables db
        sql = "SELECT table_name FROm v_catalog.tables WHERE table_schema ='#{db}' and table_name LIKE 'temp_%';"
        result = Myreplicator::DB.exec_sql("vertica",sql)
        result.rows.each do |row|
          tb = row[:table_name]
        
          if tb.size > 15
            time_str = tb[(tb.size-15)..(tb.size-1)]
            begin
              time = Time.local(time_str[0..3], time_str[4..5], time_str[6..7], time_str[9..10], time_str[11..12], time_str[13..14])
            rescue Exception => e
              puts e.message
              next
            end
            if time < Time.now() - 1.day
              sql = "DROP TABLE IF EXISTS #{db}.#{tb} CASCADE;"
              Myreplicator::DB.exec_sql("vertica",sql)
            end
          end
        end
      end
      
      def get_analyze_constraints *args
        options = args.extract_options!
        exp = Export.find(options[:export_id])
        Kernel.p "!!!!! get_analyze_constraints !!!!!"
        begin
          if exp.analyze_constraints == true
            sql = "SELECT analyze_constraints('#{options[:vertica_db]}.#{options[:vertica_schema]}.#{options[:table]}');"
            result = Myreplicator::DB.exec_sql("vertica",sql)
            if result.entries.size > 0
              return 1
            end
            sql = "SELECT COUNT(*) FROM #{options[:vertica_schema]}.#{options[:table]} WHERE modified_date < '#{(DateTime.now() -1.hour).strftime('%Y-%m-%d %H:%M:%S')}';"
            source_count = Myreplicator::DB.exec_sql("#{options[:source_schema]}",sql)
            target_count = Myreplicator::DB.exec_sql("vertica",sql)
            if source_count != target_count
              return 1
            end
          end
        rescue Exception => e
          puts e.message
        end  
        return 0      
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

