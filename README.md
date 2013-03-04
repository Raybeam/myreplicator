myreplicator
============

Rails engine that can replace mysql replication with flat-file based replication.

--------------------------

Installation
-----------

    gem install myreplicator


Configuration
---------------------------
* Create a yaml file called myreplicator.yml under the config folder in your rails app
* Set the temporary file storage path for the replicator to use "tmp_path"
* Database configurations should be stored in database.yml file
* Databases that need to be replicated should be marked by adding key  myreplicator: true to the database.yml file.
* The code/UI use the database names from database.yml file to connect to the correct source database.
 
Available configuartions for the engine:

* Myreplicator.app_root # host rails application root
* Myreplicator.tmp_path # location for storing temporary files
* Myreplicator.mysql # mysql command
* Myreplicator.mysqldump # mysqldump path
* Myreplicator.configs # yaml file
* Myreplicator.auth_required # engine authentication
* Myreplicator.authenticated # Flag for authentication
* Myreplicator.login_redirect # redirect after authentication

Sample Myreplicator Yaml file
---------------------------
	myreplicator:
	  tmp_path: # use this if you do not want to store files under tmp
	  mysqldump: mysqldump (command for mysqldump)
	  mysqlimport: mysqlimport # (command for mysqlimport)
	  mysql: mysql # (command for mysql)
	  outfile_location: /tmp/myreplicator # (for export to outfile)
	  escape_by: '"'
	  terminate_by: '\t'
	  enclosed_by: '"'
	  lines_terminate_by: '\n'
	  login_redirect: /

	  # same as the name of database in database.yml
	  remote_db_1:
	    ssh_host: localhost
	    ssh_user: guest
	    ssh_password: sasanguest
	    ssh_tmp_dir: /home/guest/tmp
  
	# Sample connection using the private key
	 remove_db_2:
	   ssh_host: 192.168.1.230
 	   ssh_user: ubuntu
 	   ssh_db_host: 127.0.0.1
	   ssh_private_key: ~/.ssh/team.pem
	   ssh_tmp_dir: /home/ubuntu/myreplicator_tmp


Sample Database Yaml file
---------------------------
	uploads:
	  adapter: mysql2
	  host: __DW_MYSQL_HOST__
	  port: __DW_MYSQL_PORT__
	  username:  __DW_MYSQL_USER__
          password: __DW_MYSQL_PASS__
          database: uploads
          myreplicator: true


Usage
-----

Once the engine is installed you need to setup Resque and Resque scheduler. Once all is setup, schedule the following jobs based on the required frequency.

         Myreplicator::Export.schedule_in_resque

    	 Resque.set_schedule("myreplicator_transporter", {
                          :cron => "5 *	* * *",
                          :class => "Myreplicator::Transporter",
                          :queue => "myreplicator_transporter"
                        })

         Resque.set_schedule("myreplicator_loader", {
                          :cron => "5 *	* * *",
                          :class => "Myreplicator::Loader",
                          :queue => "myreplicator_loader"
                        })
         end
