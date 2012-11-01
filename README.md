myreplicator
============

Rails engine that can replace mysql replication

--------------------------

Configuration
---------------------------
* Create a yaml file called myreplicator.yml under the config folder in your rails app
* Set the temporary filse storage path for the replicator to use "tmp_path"

You can configure the settings manually by:

MyEngine.config do |config|
  config.some_configuration_option = "Whatever"
end

Sample Yaml file
---------------------------
    myreplicator:
      tmp_path: tmp/myreplicator
 

Installation
-----------

    gem install myreplicator


Usage
-----

    require 'github/markup'
    GitHub::Markup.render('README.markdown', "* One\n* Two")

Or, more realistically:

    require 'github/markup'
    GitHub::Markup.render(file, File.read(file))