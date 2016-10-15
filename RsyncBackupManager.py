import os
import subprocess
import BackupFileManager as BFM
import tarfile
import shutil
import uuid


class RsyncBackupManager:

    RSYNC_DUMP_LOC = "rsync"

    def __init__(self, backup_name, backup_config):
        self.backup_name = backup_name

        print backup_name
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



    def backup(self):
        if self.backup_file_manager.backup_needed():
            print "Backup is needed, starting to download"
            self.backup_file_manager.create_all_needed_dirs()

            rsync_download_loc = self.backup_file_manager.get_current_location()
            command = "rsync -rthvz --delete " + self.rsync_options + " -e 'ssh -i " + self.keyfile + "' " + \
                      self.user + "@" + self.host + ":" + self.directory + " ."

            print command
            subprocess.call(command, shell=True,
                            cwd=rsync_download_loc)  # move to the directory and tell rsync to download to that location
            print "Finished rsync copy"

            rsync_temp_loc = self.backup_file_manager.get_tmp_location()
            if self.backup_format == "tgz":
                rsync_temp_filename = os.path.join(rsync_temp_loc, uuid.uuid4().hex + ".tgz")
                print "Gzipping rsync files"
                with tarfile.open(rsync_temp_filename, "w:gz") as tar:
                    for name in os.listdir(rsync_download_loc):
                        tar.add(os.path.join(rsync_download_loc, name), name)

                self.backup_file_manager.create_backups_as_needed(rsync_temp_filename)

                print "Removing temporary gzipped file"
                print "rm " + rsync_temp_filename
                os.unlink(rsync_temp_filename)
                print "Finished filesystem backup"

            else:
                exit("No support for anything else atm :(")

        else:
            print "No backup needed, doing nothing!"
