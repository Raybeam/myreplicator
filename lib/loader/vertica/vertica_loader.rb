module Myreplicator
  class VerticaLoader
    class << self

      def create_table *args
        options = args.extract_options!
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
      def apply_schema_change options
        #create a temp table
        temp_table = "temp" + options[:table] + DateTime.now.strftime('%Y%m%d_%H%M%S').to_s
        Myreplicator::VerticaLoader.create_table({:mysql_schema => options[:mysql_schema],
                                                            :vertica_db => options[:db], 
                                                            :vertica_schema => options[:schema],
                                                            :table => temp_table
                                                          })
        #load : from the file or dump the whole source table and then load?
                                                          
        #drop
                                                          
        #rename
      end

      

      # def create_all_tables db
      #   tables = Loader::SourceDb.get_tables(db)
      #   sqls = {}
      #   tables.each do |table|
      #     puts "Creating #{db}.#{table}"
      #     sql = "DROP TABLE IF EXISTS #{db}.#{table} CASCADE;"
      #     VerticaDb::Base.connection.execute sql
      #     sql = Loader::VerticaLoader.create_table(:vertica_db => "bidw",
      #     :vertica_table => table,
      #     :vertica_schema => db,
      #     :table => table,
      #     :db => db)
      #     sqls["#{table}"] = sql
      #     VerticaDb::Base.connection.execute sql
      #   end
      # end
    
      def prepare_options *args
        options = args.extract_options!
        options.reverse_merge!(
          :host => "sfo-load-dw-01",
          :user => "vertica",
          :pass => "test",
          :db => "bidw",
          :schema => "test",
          :table => "",
          :file => "",
          :delimiter => "\t",
          :null_value => "NULL",
          :enclosed => ""
        )
        return options  
      end
      
      # Loader::VerticaLoader.load({:schema => "king", :table => "category_overview_data", :file => "tmp/vertica/category_overview_data.tsv", :null_value => "NULL"})
      def load *args
        options = args.extract_options!
        Kernel.p options
        
        #options = {:table => "app_csvs", :db => "public", :source_schema => "okl_dev"}
        schema_check = Myreplicator::MysqlExporter.schema_changed?(:table => options[:table], 
                                                     :destination_schema => options[:db], 
                                                     :source_schema => options[:source_schema])
        
        begin
          if schema_check[:new]
              Myreplicator::VerticaLoader.create_table({:mysql_schema => schema_check[:mysql_schema],
                                                    :vertica_db => options[:db], 
                                                    :vertica_schema => options[:schema],
                                                    :table => options[:table]
                                                  })
          elsif schema_check[:changed]
            apply_schema_change({:mysql_schema => schema_check[:mysql_schema],
                                 :vertica_db => options[:db],
                                 :vertica_schema => options[:schema],
                                 :table => options[:table]
          }) 
                   
          end
        rescue Exception => e
          raise e.message
        end

        list_of_nulls =  ["0000-00-00"]
        prepared_options = prepare_options options
        Kernel.p options
        Kernel.p prepared_options
        Kernel.p prepared_options[:file]
        if prepared_options[:file].blank?
          raise "No input file"
        end
      
        begin
          process_file(:file => prepared_options[:file], :list_of_nulls => list_of_nulls, :null_value => prepared_options[:null_value])
          cmd = get_vsql_command(prepared_options)
          Kernel.p cmd
          system(cmd)
        rescue Exception => e
          raise e.message
        end
      end
        
      def get_vsql_command prepared_options
        file_extension = prepared_options[:file].split('.').last
        file_handler = ""
        file_handler = "GZIP" if file_extension == "gz" 
        sql = "COPY #{prepared_options[:schema]}.#{prepared_options[:table]} FROM LOCAL \'#{prepared_options[:file]}\' #{file_handler} DELIMITER E\'#{prepared_options[:delimiter]}\' NULL as \'#{prepared_options[:null_value]}\' ENCLOSED BY \'#{prepared_options[:enclosed]}\' EXCEPTIONS 'tmp/vertica/load_exceptions.log';"
        cmd = "/opt/vertica/bin/vsql -h #{prepared_options[:host]} -U #{prepared_options[:user]} -w #{prepared_options[:pass]} -d #{prepared_options[:db]} -c \"#{sql}\""
        return cmd
      end
      
      def process_file *args
        ### replace the null values in the input file 
        options = args.extract_options!
        options[:file].blank? ? return : file = options[:file]
        options[:list_of_nulls].blank? ? list_of_nulls = [] : list_of_nulls = options[:list_of_nulls]
        options[:null_value].blank? ? null_value = "NULL" : null_value = options[:null_value]
        
        file_extension = file.split('.').last
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
        cmd = "gunzip -f #{file} -c > tmp/temp.txt"
        system(cmd)
        # sed
        replace_null(file, list_of_nulls, null_value)
        # zip
        cmd2 = "gzip tmp/temp.txt -c > #{file}"
        system(cmd2)
      end

    end
  end
end

