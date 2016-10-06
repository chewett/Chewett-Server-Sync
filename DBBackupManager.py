from WookieDb import WookieDb
import re
import os
import uuid
import shutil
import BackupFileManager as BFM
import tarfile


class DBBackupManager:

    DB_DUMP_LOCATION = "dbs"

    def __init__(self, backup_name, backup_config):
        print backup_name
        self.backup_name = backup_name
        self.backup_config = backup_config

    def backup(self):

        db = WookieDb.WookieDb(host=self.backup_config['host'], user=self.backup_config['user'],
                               password=self.backup_config['password'], db=self.backup_config['schema'])
        tables = [t[0] for t in db.show_tables()]
        print "found " + str(len(tables)) + " tables"

        if "table_whitelist" in self.backup_config:
            whitelist = self.backup_config['table_whitelist']
            new_tables = []
            for table in tables:
                add_table = False
                for whitelist_value in whitelist:
                    pattern = re.compile(whitelist_value)
                    if pattern.match(table):
                        add_table = True
                        break

                if add_table:
                    new_tables.append(table)

            print "Whitelist ran: " + str(len(new_tables)) + " tables to download after processing whitelist, " + str(
                len(tables) - len(new_tables)) + " tables filtered out"
            tables = new_tables

        if "store_location" in self.backup_config:
            backup_location = os.path.join(self.backup_config['store_location'])
        else:
            if not os.path.isdir(DBBackupManager.DB_DUMP_LOCATION):
                os.mkdir(DBBackupManager.DB_DUMP_LOCATION)

            backup_location = os.path.join(DBBackupManager.DB_DUMP_LOCATION, self.backup_name)

        if not os.path.isdir(backup_location):
            os.mkdir(backup_location)

        db_download_loc = os.path.join(backup_location, "tmp")
        if not os.path.isdir(db_download_loc):
            os.mkdir(db_download_loc)

        download_loc = os.path.join(db_download_loc, uuid.uuid4().hex)
        if not os.path.isdir(download_loc):
            os.mkdir(download_loc)
        else:
            shutil.rmtree(download_loc)
            os.mkdir(download_loc)

        day_filename = BFM.get_day_filename(self.backup_name)
        week_filename = BFM.get_week_filename(self.backup_name)
        month_filename = BFM.get_month_filename(self.backup_name)

        BFM.create_day_week_month_dirs(backup_location)
        day_backup_loc = BFM.get_day_backup_location(backup_location)

        day_backup = os.path.join(day_backup_loc, day_filename)
        if not os.path.isfile(day_backup):
            copy_day_file = True
        else:
            copy_day_file = False

        week_backup_loc = BFM.get_week_backup_location(backup_location)

        week_backup = os.path.join(week_backup_loc, week_filename)
        if not os.path.isfile(week_backup):
            copy_week_file = True
        else:
            copy_week_file = False

        month_backup_loc = BFM.get_month_backup_location(backup_location)

        month_backup = os.path.join(month_backup_loc, month_filename)
        if not os.path.isfile(month_backup):
            copy_month_file = True
        else:
            copy_month_file = False

        if copy_day_file or copy_week_file or copy_month_file:
            db.dump_tables(tables, download_loc)

            day_file_to_save = os.path.join(db_download_loc, day_filename)

            with tarfile.open(day_file_to_save, "w:gz") as tar:
                for name in os.listdir(download_loc):
                    tar.add(os.path.join(download_loc, name), name)

            if copy_day_file:
                shutil.copy(day_file_to_save, day_backup)
            if copy_week_file:
                shutil.copy(day_file_to_save, week_backup)
            if copy_month_file:
                shutil.copy(day_file_to_save, month_backup)

            shutil.rmtree(download_loc)
            os.unlink(day_file_to_save)