class CreateMyreplicatorVerticaExports < ActiveRecord::Migration
  def change
    create_table :myreplicator_vertica_exports do |t|
      t.string :database
      t.string :schema
      t.string :table_name
      t.string :file_path
      t.string :file_format
      t.string :delimiter
      t.string :field_enclosed_by
      t.string :line_terminated_by
      t.string :cron
      t.string :state, :default => "new"
      t.text :error
      t.boolean :active, :default => true
      t.integer :exporter_pid
      t.datetime :export_started_at, :default => nil
      t.datetime :export_finished_at, :default => nil
      t.timestamps
    end
  end
end
