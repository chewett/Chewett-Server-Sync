import os
from Compressor import Compressor
import BackupFileManager as BFM
from CommandRunner import run_command
import uuid


class RsyncBackupManager:

    RSYNC_DUMP_LOC = "rsync"

    def __init__(self, backup_name, backup_config, general_settings):
        self.backup_name = backup_name

        self.backup_name = backup_name

        if "store_location" in backup_config:
            backup_location = backup_config['store_location']
        else:
            backup_location = os.path.join(RsyncBackupManager.RSYNC_DUMP_LOC, self.backup_name)

        if "user" in backup_config:
            self.user = backup_config["user"]

        if "host" in backup_config:
            self.host = backup_config["host"]

        if "keyfile" in backup_config:
            self.keyfile = backup_config["keyfile"]
        else:
            self.keyfile = None

        if "directory" in backup_config:
            self.directory = backup_config["directory"]

        if "rsync_options" in backup_config:
            self.rsync_options = backup_config['rsync_options']
        else:
            self.rsync_options = ""  # by default, no more

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
                                                         self.daily_backups, self.weekly_backups, self.monthly_backups,
                                                         current_folder_needed=True)

        if 'tmp_folder' in general_settings:
            self.tmp_folder = general_settings['tmp_folder']
        else:
            self.tmp_folder = self.backup_file_manager.get_tmp_location()

        compression_type = "python"
        if "compression" in general_settings:
            compression_type = general_settings['compression']

        self.compressor = Compressor(compression_type)


    def backup(self):
        if self.backup_file_manager.backup_needed():
            print("Backup is needed, starting to download")
            self.backup_file_manager.create_all_needed_dirs()

            keyfile_cmd = ""
            if self.keyfile is not None:
                keyfile_cmd = " -i " + self.keyfile

            rsync_download_loc = self.backup_file_manager.get_current_location()
            command = "rsync -rthz --delete " + self.rsync_options + " -e 'ssh" + keyfile_cmd + "' " + \
                      self.user + "@" + self.host + ":" + self.directory + " ."

            # move to the directory and tell rsync to download to that location
            run_command(command, cwd=rsync_download_loc)
            print("Finished rsync copy")

            if self.backup_format == "tgz":
                rsync_temp_filename = os.path.join(self.tmp_folder, uuid.uuid4().hex)
                rsync_temp_filename = self.compressor.compress(rsync_download_loc, rsync_temp_filename)
                self.backup_file_manager.create_backups_as_needed(rsync_temp_filename)

                print("Removing temporary gzipped file")
                print("rm " + rsync_temp_filename)
                os.unlink(rsync_temp_filename)
                print("Finished filesystem backup")

            else:
                exit("No support for anything else atm :(")

        else:
            print("No backup needed for rsync backup " + self.backup_name)
