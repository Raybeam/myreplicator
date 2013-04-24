module Myreplicator
  class VerticaSql
  
    def self.create_table_stmt options
      sql = "CREATE TABLE IF NOT EXISTS #{options[:vertica_db]}."
      sql += "#{options[:vertica_schema]}." if options[:vertica_schema]
      sql += "#{options[:table]} ("

      index = 1
      #primary_set = false

      options[:columns].each do |column|
        sql += "\"#{column['column_name']}\" "

        sql += data_type(column['data_type'], column['column_type'])
        sql += " "

        sql += nullable(column['is_nullable'])
        sql += " "

        if index < options[:columns].size
          sql += ", "
        end
        index += 1
      end
      
      # Add primary key
      primary_set = false
      options[:columns].each do |column|
        if column['column_key'] == "PRI"
          if !primary_set
            sql += ", PRIMARY KEY (" + "\"#{column['column_name']}\" "
          else
            sql += ", " + "\"#{column['column_name']}\" "
          end
          primary_set = true
        end
      end
      if primary_set
        sql += ") "
      end
      
      # Add unique key
      options[:columns].each do |column|
        if column['column_key'] == "UNI"
          sql += ", UNIQUE (" + "\"#{column['column_name']}\" "
          sql += ") "
        end
      end
      
      sql += ");"
      puts sql
    
      return sql
    end
  
    def self.nullable is_nullable
      if is_nullable == "YES"
        return "NULL"
      elsif is_nullable == "NO"
        return "NOT NULL"
      end
      return ""
    end
 
    def self.data_type type, col_type
      type = Myreplicator::VerticaTypes.convert type, col_type
      result = " #{type} "
      return result
    end

    def self.key col_key
      col_key = Myreplicator::VerticaTypes.convert_key col_key
      return  "#{col_key} "
    end
  end
end
