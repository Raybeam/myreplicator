module Myreplicator
  class VerticaUtils
    class << self
      # Example: get_grant({:db => "bidw", :schema => "king", :table => "customer"})
      def get_grants *args
        options = args.extract_options!
        db = options[:db]
        schema = options[:schema]
        table = options[:table]
        sql = "SELECT * FROM grants WHERE object_schema = '#{schema}' AND object_name = '#{table}';"
        result = Myreplicator::DB.exec_sql("vertica",sql)
        sqls = []
        result.entries.each do |priv|
          privilege = priv[:privileges_description]
          grantee = priv[:grantee]
          begin
            sql = "GRANT #{privilege} ON #{schema}.#{table} TO #{grantee};"
            sqls << sql
            puts sql
          rescue Exception => e
            puts e.message
          end
        end
        return sqls
      end
    
      # Example: set_grants sqls
      def set_grants sqls
        sqls.each do |sql|
          begin
            puts sql
            Myreplicator::DB.exec_sql("vertica",sql)
          rescue Exception => e
            puts e.message
          end
        end
      end
    
      # Example: save_grants_to_file({:sqls => ["GRANT ..","GRANT ..."], :file=>"grants.txt"})
      def save_grants_to_file *args
        options = args.extract_options!
        sqls = options[:sqls]
        filename = Rails.root.join('tmp', options[:file])
        file = File.open(filename, "w+")
        sqls.each do |sql|
          file.puts sql.to_s
        end
        file.close()
      end
    
      def load_grants_from_file f
        file = Rails.root.join('tmp', f)
        file = File.open(filename, "r")
        sqls = file.readlines
        file.close()
        return sqls
      end
      
    end #end class << self
  end
end