import os
import subprocess
import BackupFileManager as BFM
import tarfile
import shutil


class RsyncBackupManager:

    RSYNC_DUMP_LOC = "rsync"

    def __init__(self, backup_name, backup_config):
        print backup_name
        self.backup_name = backup_name
        self.backup_config = backup_config

    def backup(self):

        if "store_location" in self.backup_config:
            backup_location = self.backup_config['store_location']
        else:
            if not os.path.isdir(RsyncBackupManager.RSYNC_DUMP_LOC):
                os.mkdir(RsyncBackupManager.RSYNC_DUMP_LOC)

            backup_location = os.path.join(RsyncBackupManager.RSYNC_DUMP_LOC, self.backup_name)

        if not os.path.isdir(backup_location):
            os.mkdir(backup_location)

        rsync_download_loc = os.path.join(backup_location, "current")
        if not os.path.isdir(rsync_download_loc):
            os.mkdir(rsync_download_loc)

        if "rsync_options" in self.backup_config:
            rsync_options = self.backup_config['rsync_options']
        else:
            rsync_options = ""  # by default, no more

        command = "rsync -rthvz --delete " + rsync_options + " -e 'ssh -i " + self.backup_config['keyfile'] + "' " + \
                  self.backup_config['user'] + "@" + self.backup_config['host'] + ":" + self.backup_config[
                      'directory'] + " ."

        print command
        subprocess.call(command, shell=True,
                        cwd=rsync_download_loc)  # move to the directory and tell rsync to download to that location

        day_filename = BFM.get_day_filename(self.backup_name)
        week_filename = BFM.get_week_filename(self.backup_name)
        month_filename = BFM.get_month_filename(self.backup_name)

        BFM.create_day_week_month_dirs(backup_location)

        day_backup_loc = BFM.get_day_backup_location(backup_location)
        week_backup_loc = BFM.get_week_backup_location(backup_location)
        month_backup_loc = BFM.get_month_backup_location(backup_location)

        backups_needed = BFM.backups_need_update(backup_location, self.backup_name)

        if backups_needed['day'] or backups_needed['week'] or backups_needed['month']:
            rsync_temp_loc = os.path.join(backup_location, "tmp")
            if not os.path.isdir(rsync_temp_loc):
                os.mkdir(rsync_temp_loc)

            day_file_to_save = os.path.join(rsync_temp_loc, day_filename)

            with tarfile.open(day_file_to_save, "w:gz") as tar:
                for name in os.listdir(rsync_download_loc):
                    tar.add(os.path.join(rsync_download_loc, name), name)

            if backups_needed['day']:
                day_backup = os.path.join(day_backup_loc, day_filename)
                shutil.copy(day_file_to_save, day_backup)
            if backups_needed['week']:
                week_backup = os.path.join(week_backup_loc, week_filename)
                shutil.copy(day_file_to_save, week_backup)
            if backups_needed['month']:
                month_backup = os.path.join(month_backup_loc, month_filename)
                shutil.copy(day_file_to_save, month_backup)

            os.unlink(day_file_to_save)
        else:
            print "No rsync needed as there are no backups"
