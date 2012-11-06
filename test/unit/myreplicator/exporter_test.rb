require 'test_helper'

module Myreplicator
  class ExporterTest < ActiveSupport::TestCase
    test "MysqlDump Cmd" do
      
      SqlCommands.mysqldump 
      assert true
    end
  end
end
