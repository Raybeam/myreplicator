# This migration comes from myreplicator (originally 20121025191622)
class CreateMyreplicatorExports < ActiveRecord::Migration
  def change
    create_table :myreplicator_exports do |t|
      t.string :source_schema
      t.string :destination_schema
      t.string :table_name
      t.string :incremental_column
      t.string :max_incremental_value
      t.string :incremental_column_type
      t.string :export_to, :default => "destination_db"
      t.string :export_type, :default => "incremental"
      t.string :s3_path
      t.string :cron
      t.string :state, :default => "new"
      t.text :error
      t.boolean :active, :default => true
      t.integer :exporter_pid
      t.integer :transporter_pid
      t.integer :loader_pid
      t.datetime :export_started_at, :default => nil
      t.datetime :export_finished_at, :default => nil
      t.datetime :load_started_at, :default => nil
      t.datetime :load_finished_at, :default => nil
      t.datetime :transfer_started_at, :default => nil
      t.datetime :transfer_finished_at, :default => nil
      t.timestamps
    end  
    add_index :myreplicator_exports, [:source_schema, :destination_schema, :table_name], :unique => true, :name => "unique_index"
  end

  def self.down
    drop_table :myreplicator_exports
  end
  
end
