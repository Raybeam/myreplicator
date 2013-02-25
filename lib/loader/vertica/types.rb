module VerticaUtils
  class VerticaTypes
  
    def self.convert type, col_type
      if mysql_vertica_conversion[type].blank?
        return col_type
      else
        return mysql_vertica_conversion[type]
      end
    end

    def self.mysql_vertica_conversion
      map = {
        "int" => "int",
        "integer" => "int",
        "int8" => "int",
        "smallint" => "int",
        "bigint" => "int",
        "tinyint" => "int",
        "numeric" => "int",
        "text" => "VARCHAR(65000)",
        "mediumtext" => "VARCHAR(65000)",
        "bit" => "binary",
        "longtext" => "VARCHAR(65000)",
        "float" => "decimal"
      }
    end

    def self.convert_key key
      map = {
        "UNI" => "UNIQUE",
      " MUL" => "", 
        "PRI" => "PRIMARY KEY"
      }

      if map[key].blank?
        return ""
      else
        return map[key]
      end
    end

  end
end