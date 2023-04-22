import os
import BackupFileManager as BFM
from Compressor import Compressor
import uuid
from CommandRunner import run_command
import shutil

class InfluxBackupManager:

    INFLUX_DUMP_LOCATION = "influx"

    def __init__(self, backup_name, backup_config, general_settings):
        self.backup_name = backup_name
        self.influx_cli = general_settings['influx_cli']

        if "host" in backup_config:
            self.host = backup_config["host"]
        else:
            self.host = None

        if "org" in backup_config:
            self.org = backup_config['org']
        else:
            self.org = None

        self.token = backup_config['token']

        if "store_location" in backup_config:
            backup_location = backup_config['store_location']
        else:
            backup_location = os.path.join(InfluxBackupManager.INFLUX_DUMP_LOCATION, self.backup_name)

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

        compression_type = "python"
        if "compression" in general_settings:
            compression_type = general_settings['compression']

        self.compressor = Compressor(compression_type)



    def backup(self):
        if not self.backup_file_manager.backup_needed():
            print("No backup needed for InfluxDB backup " + self.backup_name)
            return

        print("InfluxDB Backup is needed, starting to download:" + self.backup_name)
        self.backup_file_manager.create_all_needed_dirs()

        my_download_loc = os.path.join(self.tmp_folder, uuid.uuid4().hex)

        host_details = ""
        if self.host:
            host_details = " --host " + self.host + " "

        org_details = ""
        if self.org:
            org_details = " --org " + self.org + " "

        #Run the backup command
        run_command(self.influx_cli + " backup " + host_details + org_details + " -t " + self.token + " " + my_download_loc )
        print("Finished InfluxDB Backup")

        if self.backup_format == "tgz":
            print("Gzipping InfluxDB backup")
            tarfile_location = self.compressor.compress(my_download_loc, my_download_loc)

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

