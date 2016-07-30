from WookieDb import WookieDb
import json
import os
import shutil
import subprocess
import time
import tarfile
import uuid
from ftplib import FTP
import re

backup_detail_file = open("backup_details.json", "r")
backup_details = json.load(backup_detail_file)

DB_DUMP_LOC = "dbs"

print backup_details

for db_backup_name in backup_details['dbs']:
    print db_backup_name
    host_backup_info = backup_details['dbs'][db_backup_name]

    db = WookieDb.WookieDb(host=host_backup_info['host'], user=host_backup_info['user'], password=host_backup_info['password'], db=host_backup_info['schema'])
    tables = [t[0] for t in db.show_tables()]
    print "found " + str(len(tables)) + " tables"

    if "table_whitelist" in host_backup_info:
        whitelist = host_backup_info['table_whitelist']
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


        print "Whitelist ran: " + str(len(new_tables)) + " tables to download after processing whitelist, " + str(len(tables) - len(new_tables)) + " tables filtered out"
        tables = new_tables


    if not os.path.isdir(DB_DUMP_LOC):
        os.mkdir(DB_DUMP_LOC)

    backup_location = os.path.join(DB_DUMP_LOC, db_backup_name)
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

    day_filename = time.strftime(db_backup_name + "_day_%Y_%m_%d.tgz")
    week_filename = time.strftime(db_backup_name + "_week_%Y_%U.tgz")
    month_filename = time.strftime(db_backup_name + "_month_%Y_%m.tgz")

    day_backup_loc = os.path.join(backup_location, "day")
    if not os.path.isdir(day_backup_loc):
        os.mkdir(day_backup_loc)

    day_backup = os.path.join(day_backup_loc, day_filename)
    if not os.path.isfile(day_backup):
        copy_day_file = True
    else:
        copy_day_file = False

    week_backup_loc = os.path.join(backup_location, "week")
    if not os.path.isdir(week_backup_loc):
        os.mkdir(week_backup_loc)

    week_backup = os.path.join(week_backup_loc, week_filename)
    if not os.path.isfile(week_backup):
        copy_week_file = True
    else:
        copy_week_file = False

    month_backup_loc = os.path.join(backup_location, "month")
    if not os.path.isdir(month_backup_loc):
        os.mkdir(month_backup_loc )

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
                tar.add(os.path.join(download_loc,name), name)

        if copy_day_file:
            shutil.copy(day_file_to_save, day_backup)
        if copy_week_file:
            shutil.copy(day_file_to_save, week_backup)
        if copy_month_file:
            shutil.copy(day_file_to_save, month_backup)

        shutil.rmtree(download_loc)
        os.unlink(day_file_to_save)

for host_backup_name in backup_details['ftp']:
    host_backup_info = backup_details['ftp'][host_backup_name]

    ftp = FTP(host_backup_info['host'], host_backup_info['user'], host_backup_info['password'])
    ftp.login()

    print ftp.retrlines('LIST')
    