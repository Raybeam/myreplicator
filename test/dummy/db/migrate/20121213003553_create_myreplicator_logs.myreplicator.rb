# This migration comes from myreplicator (originally 20121212003652)
class CreateMyreplicatorLogs < ActiveRecord::Migration
  def change
    create_table :myreplicator_logs do |t|
      t.integer :pid
      t.string :job_type
      t.string :name
      t.string :file
      t.string :state
      t.string :thread_state
      t.string :hostname
      t.string :export_id
      t.text :error
      t.text :backtrace
      t.string :guid
      t.datetime :started_at, :default => nil
      t.datetime :finished_at, :default => nil

      t.timestamps
    end
  end
end
