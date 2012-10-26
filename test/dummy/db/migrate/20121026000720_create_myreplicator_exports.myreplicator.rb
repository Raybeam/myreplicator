# This migration comes from myreplicator (originally 20121025191622)
class CreateMyreplicatorExports < ActiveRecord::Migration
  def change
    create_table :myreplicator_exports do |t|
      t.string :source_schema
      t.string :destination_schema
      t.string :table_name
      t.string :incremental_column
      t.string :max_incremental_value
      t.string :export_to, :default => "destination_db"
      t.string :export_type, :default => "incremental"
      t.string :s3_path
      t.string :cron
      t.boolean :active, :default => true
      t.timestamps
    end

    def self.down
      drop_table :myreplicator
    end

  end
end
