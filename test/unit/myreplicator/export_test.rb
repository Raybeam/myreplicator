require 'test_helper'

module Myreplicator
  class ExportTest < ActiveSupport::TestCase
    test "the truth" do
      Kernel.p Export.all
      assert true
    end
  end
end
