import re
import os
import uuid
import shutil
import BackupFileManager as BFM
import tarfile
import mysql.connector
import sshtunnel
import MysqldumpWrapper


class DBBackupManager:

    DB_DUMP_LOCATION = "dbs"

    def __init__(self, backup_name, backup_config, general_settings):
        self.backup_name = backup_name
        self.mysql_dump_wrapper = MysqldumpWrapper.MysqldumpWrapper(general_settings['mysqldump'])

        if "host" in backup_config:
            self.host = backup_config["host"]
        else:
            self.host = "localhost"

        if "user" in backup_config:
            self.user = backup_config["user"]
        else:
            self.user = "root"

        if "password" in backup_config:
            self.password = backup_config["password"]
        else:
            self.password = ""

        if "schema" in backup_config:
            self.schema = backup_config["schema"]

        if "port" in backup_config:
            self.port = backup_config["port"]
        else:
            self.port = 3306

        if "store_location" in backup_config:
            backup_location = os.path.join(backup_config['store_location'])
        else:
            backup_location = os.path.join(DBBackupManager.DB_DUMP_LOCATION, self.backup_name)

        if "backup_file_format" in backup_config:
            self.backup_format = backup_config["backup_file_format"]
        else:
            self.backup_format = "tgz"

        if "daily_backups" in backup_config:
            self.daily_backups = backup_config["daily_backups"]
        else:
            self.daily_backups = 7

        if "weekly_backups" in backup_config:
            self.weekly_backups = backup_config["weekly_backups"]
        else:
            self.weekly_backups = 12

        if "monthly_backups" in backup_config:
            self.monthly_backups = backup_config["monthly_backups"]
        else:
            self.monthly_backups = -1

        self.backup_file_manager = BFM.BackupFileManager(self.backup_name, backup_location, self.backup_format,
                                                         self.daily_backups, self.weekly_backups,self.monthly_backups)

        if 'tmp_folder' in general_settings:
            self.tmp_folder = general_settings['tmp_folder']
        else:
            self.tmp_folder = self.backup_file_manager.get_tmp_location()

        if "table_whitelist" in backup_config:
            self.table_whitelist = backup_config["table_whitelist"]
        else:
            self.table_whitelist = None

        self.use_ssh = False
        if "ssh_host" in backup_config and "ssh_user" in backup_config and "ssh_keyfile" in backup_config and "ssh_port" in backup_config:
            self.use_ssh = True
            self.ssh_host = backup_config['ssh_host']
            self.ssh_user = backup_config['ssh_user']
            self.ssh_port = backup_config['ssh_port']
            self.ssh_keyfile = backup_config['ssh_keyfile']

    def backup(self):
        if self.backup_file_manager.backup_needed():
            print("Backup is needed, starting to download")
            self.backup_file_manager.create_all_needed_dirs()

            local_port_to_use = self.port
            if self.use_ssh:
                tunnel = sshtunnel.SSHTunnelForwarder((self.ssh_host, self.ssh_port), ssh_pkey=self.ssh_keyfile, ssh_username=self.ssh_user,
                                            remote_bind_address=("127.0.0.1", self.port))
                tunnel.start()

                db = mysql.connector.MySQLConnection(
                    user=self.user,
                    password=self.password,
                    host="127.0.0.1",
                    database=self.schema,
                    port=tunnel.local_bind_port)

                local_port_to_use = tunnel.local_bind_port

            else:
                db = mysql.connector.connect(host=self.host, user=self.user, password=self.password, db=self.schema)

            c = db.cursor()
            c.execute("SHOW TABLES")
            tables = [t[0] for t in c.fetchall()]
            print("Found " + str(len(tables)) + " tables")

            if self.table_whitelist:
                new_tables = []
                for table in tables:
                    add_table = False
                    for whitelist_value in self.table_whitelist:
                        pattern = re.compile(whitelist_value)
                        if pattern.match(table):
                            add_table = True
                            break

                    if add_table:
                        new_tables.append(table)

                print("Whitelist ran: " + str(len(new_tables)) + " tables to download after processing whitelist, " + str(
                    len(tables) - len(new_tables)) + " tables filtered out")
                tables = new_tables

            my_download_loc = os.path.join(self.tmp_folder, uuid.uuid4().hex)
            if not os.path.isdir(my_download_loc):
                os.mkdir(my_download_loc)
            else:
                shutil.rmtree(my_download_loc)
                os.mkdir(my_download_loc)

            self.mysql_dump_wrapper.dump_tables_to_file(self.host, local_port_to_use, self.user, self.password, self.schema, tables, my_download_loc)

            print("Finished database dump")
            if self.backup_format == "tgz":
                print("Gzipping database tables")
                tarfile_location = my_download_loc + ".tgz"
                with tarfile.open(tarfile_location, "w:gz") as tar:
                    for name in os.listdir(my_download_loc):
                        tar.add(os.path.join(my_download_loc, name), name)

                self.backup_file_manager.create_backups_as_needed(tarfile_location)
                print("removing temporary tarred file")
                print("rm " + tarfile_location)
                os.unlink(tarfile_location)

            else:
                exit("No support for anything else atm :(")

            print("removing download directory")
            print("rm -rf " + my_download_loc)
            shutil.rmtree(my_download_loc)
            print("Finished database backup")

        else:
            print("No backup needed, doing nothing!")
