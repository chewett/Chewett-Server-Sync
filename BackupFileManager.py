import os
import time
import shutil
import glob
import re


class BackupFileManager:

    '''
        backup_filetype = ["tgz", ""]
    '''
    def __init__(self, backup_name, backup_location, backup_filetype, daily_num, weekly_num, monthly_num, current_folder_needed=False):
        self.backup_name = backup_name
        self.backup_location = backup_location
        self.backup_filetype = backup_filetype
        self.daily_num = daily_num
        self.weekly_num = weekly_num
        self.monthly_num = monthly_num
        self.current_folder_needed = current_folder_needed

    def get_backup_file_extension(self):
        if self.backup_filetype == "tgz":
            return ".tgz"
        else:
            return ""

    def create_backups_as_needed(self, backup_file):
        backups_needed = self.backups_need_update()

        if backups_needed['day']:
            full_day_path = self.get_day_full_path()
            print("Copying backup file to day path")
            print("cp " + backup_file + " " + full_day_path)
            shutil.copy(backup_file, full_day_path)

        if backups_needed['week']:
            full_week_path = self.get_week_full_path()
            print("Copying backup file to week path")
            print("cp " + backup_file + " " + full_week_path)
            shutil.copy(backup_file, full_week_path)

        if backups_needed['month']:
            full_month_path = self.get_month_full_path()
            print("Copying backup file to month path")
            print("cp " + backup_file + " " + full_month_path)
            shutil.copy(backup_file, full_month_path)

        self.manage_previous_backups()

    def manage_previous_backups(self):
        if self.daily_num and self.daily_num != -1:
            day_glob_filename = os.path.join(self.get_day_backup_location(),
                                              self.backup_name + "_day_*" + self.get_backup_file_extension())

            day_files = glob.glob(day_glob_filename)
            day_files.sort()
            if len(day_files) > self.daily_num:
                num_to_remove = len(day_files) - self.daily_num
                for i in range(num_to_remove):
                    file_path_to_remove = day_files[i]
                    print("Removing file due to number of archives exceeded" + file_path_to_remove)
                    self.remove_backup(file_path_to_remove)


        if self.weekly_num and self.weekly_num != -1:
            week_glob_filename = os.path.join(self.get_week_backup_location(),
                                             self.backup_name + "_week_*" + self.get_backup_file_extension())

            week_files = glob.glob(week_glob_filename)
            week_files.sort()
            if len(week_files) > self.weekly_num:
                num_to_remove = len(week_files) - self.weekly_num
                for i in range(num_to_remove):
                    file_path_to_remove = week_files[i]
                    print("Removing file due to number of archives exceeded" + file_path_to_remove)
                    self.remove_backup(file_path_to_remove)

        if self.monthly_num and self.monthly_num != -1:
            month_glob_filename = os.path.join(self.get_month_backup_location(),
                                             self.backup_name + "_month_*" + self.get_backup_file_extension())

            month_files = glob.glob(month_glob_filename)
            month_files.sort()
            if len(month_files) > self.monthly_num:
                num_to_remove = len(month_files) - self.monthly_num
                for i in range(num_to_remove):
                    file_path_to_remove = month_files[i]
                    print("Removing file due to number of archives exceeded" + file_path_to_remove)
                    self.remove_backup(file_path_to_remove)

    def remove_backup(self, path):
        if os.path.isdir(path):
            shutil.rmtree(path)
            os.unlink(path)
        else:
            os.unlink(path)

    def get_day_filelocation(self):
        return time.strftime(self.backup_name + "_day_%Y_%m_%d" + self.get_backup_file_extension())

    def get_week_filelocation(self):
        return time.strftime(self.backup_name + "_week_%Y_%W" + self.get_backup_file_extension())

    def get_month_filelocation(self):
        return time.strftime(self.backup_name + "_month_%Y_%m" + self.get_backup_file_extension())

    def get_current_location(self):
        return os.path.join(self.backup_location, "current")

    def get_tmp_location(self):
        return os.path.join(self.backup_location, "tmp")

    def get_day_backup_location(self):
        return os.path.join(self.backup_location, "day")

    def get_week_backup_location(self):
        return os.path.join(self.backup_location, "week")

    def get_month_backup_location(self):
        return os.path.join(self.backup_location, "month")

    def get_day_full_path(self):
        return os.path.join(self.get_day_backup_location(), self.get_day_filelocation())

    def get_week_full_path(self):
        return os.path.join(self.get_week_backup_location(), self.get_week_filelocation())

    def get_month_full_path(self):
        return os.path.join(self.get_month_backup_location(), self.get_month_filelocation())

    def create_all_needed_dirs(self):
        locations_to_create = [self.backup_location, self.get_tmp_location()]

        if self.current_folder_needed:
            locations_to_create.append(self.get_current_location())
        if self.daily_num:
            locations_to_create.append(self.get_day_backup_location())
        if self.weekly_num:
            locations_to_create.append(self.get_week_backup_location())
        if self.monthly_num:
            locations_to_create.append(self.get_month_backup_location())

        for loc in locations_to_create:
            if not os.path.isdir(loc):
                os.makedirs(loc)

    def backup_needed(self):
        all_backups_needed = self.backups_need_update()
        return (all_backups_needed['day'] or all_backups_needed['week']  or all_backups_needed['month'])

    def backups_need_update(self):

        if self.daily_num:
            day_backup_loc = self.get_day_backup_location()
            day_backup = os.path.join(day_backup_loc, self.get_day_filelocation())
            if not os.path.isfile(day_backup):
                copy_day_file = True
            else:
                copy_day_file = False
        else:
            copy_day_file = False

        if self.weekly_num:
            week_backup_loc = self.get_week_backup_location()
            week_backup = os.path.join(week_backup_loc, self.get_week_filelocation())
            if not os.path.isfile(week_backup):
                copy_week_file = True
            else:
                copy_week_file = False
        else:
            copy_week_file = False

        if self.monthly_num:
            month_backup_loc = self.get_month_backup_location()
            month_backup = os.path.join(month_backup_loc, self.get_month_filelocation())
            if not os.path.isfile(month_backup):
                copy_month_file = True
            else:
                copy_month_file = False
        else:
            copy_month_file = False

        return {
            "day": copy_day_file,
            "week": copy_week_file,
            "month": copy_month_file
        }
