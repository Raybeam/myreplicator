class CreateMyreplicatorTransfers < ActiveRecord::Migration
  def change
    create_table :myreplicator_transfers do |t|

      t.timestamps
    end
  end
end
