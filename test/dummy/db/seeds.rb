require 'active_record/fixtures'

["batchy_batches"].each do |tname|
  puts tname
  Myreplicator::Export.find_or_create_by_table_name!(:table_name => tname,
                                                     :source_schema => "okl_test",
                                                     :destination_schema => "myreplicator",
                                                     :incremental_column => "updated_at",    
                                                     :cron => "2 * * * *")
end

# ActiveRecord::Fixtures.create_fixtures("#{Rails.root}/test/fixtures", "myreplicator_exports")
