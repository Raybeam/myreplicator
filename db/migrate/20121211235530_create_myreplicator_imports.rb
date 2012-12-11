class CreateMyreplicatorImports < ActiveRecord::Migration
  def change
    create_table :myreplicator_imports do |t|

      t.timestamps
    end
  end
end
