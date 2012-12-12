class CreateMyreplicatorTransfers < ActiveRecord::Migration
  def change
    create_table :myreplicator_transfers do |t|
      t.integer :transporter_pid
      t.string :state
      t.string :hostname
      t.string :filename
      t.string :export_id
      t.text :error
      t.text :backtrace
      t.string :guid
      t.datetime :transfer_started_at, :default => nil
      t.datetime :transfer_finished_at, :default => nil
      t.timestamps
    end
  end
end
