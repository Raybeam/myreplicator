module Myreplicator
  class VerticaSql
  
    def self.create_table_stmt options

      sql = "CREATE TABLE IF NOT EXISTS #{options[:vertica_db]}."
      sql += "#{options[:vertica_schema]}." if options[:vertica_schema]
      sql += "#{options[:table]} ("

      index = 1
      primary_set = false

      options[:columns].each do |column|
        sql += "\"#{column['column_name']}\" "

        sql += data_type(column['data_type'], column['column_type'])
        sql += " "

        if column['column_key'] == "PRI"
          sql += key(column['column_key']) + " " unless primary_set # set only one primary key
          primary_set = true
        end

        sql += nullable(column['is_nullable'])
        sql += " "

        if index < options[:columns].size
          sql += ", "
        else
          sql += ");"
        end
        index += 1
      end
 
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
      type = VerticaTypes.convert type, col_type
      result = " #{type} "
      return result
    end

    def self.key col_key
      col_key = VerticaTypes.convert_key col_key
      return  "#{col_key} "
    end
  end
end
