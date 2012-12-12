class CreateMyreplicatorImports < ActiveRecord::Migration
  def change
    create_table :myreplicator_imports do |t|
      t.integer :loader_pid
      t.string :state
      t.string :hostname
      t.string :filename
      t.string :export_id
      t.text :error
      t.text :backtrace
      t.string :guid
      t.datetime :load_started_at, :default => nil
      t.datetime :load_finished_at, :default => nil
      t.timestamps
    end
  end
end
