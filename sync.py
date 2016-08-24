from WookieDb import WookieDb
import json
import os
import shutil
import subprocess
import time
import tarfile
import uuid
from ftplib import FTP
import ftplib
import re
import BackupFileManager as BFM

backup_detail_file = open("backup_details.json", "r")
backup_details = json.load(backup_detail_file)

DB_DUMP_LOC = "dbs"
FTP_DUMP_LOC = "ftps"
RSYNC_DUMP_LOC = "./rsync"

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

    if "store_location" in host_backup_info:
        backup_location = os.path.join(host_backup_info['store_location'])
    else:
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

    day_filename = BFM.get_day_filename(db_backup_name)
    week_filename = BFM.get_week_filename(db_backup_name)
    month_filename = BFM.get_month_filename(db_backup_name)

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

    if not os.path.isdir(FTP_DUMP_LOC):
        os.mkdir(FTP_DUMP_LOC)

    backup_location = os.path.join(DB_DUMP_LOC, host_backup_name)
    if not os.path.isdir(backup_location):
        os.mkdir(backup_location)

    ftp_download_loc = os.path.join(backup_location, "tmp")
    if not os.path.isdir(ftp_download_loc):
        os.mkdir(ftp_download_loc)

    download_loc = os.path.join(ftp_download_loc, uuid.uuid4().hex)
    if not os.path.isdir(download_loc):
        os.mkdir(download_loc)
    else:
        shutil.rmtree(download_loc)
        os.mkdir(download_loc)


    ftp = FTP(host_backup_info['host'], host_backup_info['user'], host_backup_info['password'])
    #ftp.login() # FIXME: Look at why I dont want this

    ftp_root_dir = host_backup_info["directory"]


    folders_to_search = [ftp_root_dir]
    files_to_download = []

    while len(folders_to_search) > 0:
        folder = folders_to_search[0]
        folders_to_search.remove(folder)

        ftp.cwd(folder)
        filelist = ftp.nlst()
        print filelist

        for file in filelist:
            try:
                # this will check if file is folder:
                ftp.cwd(folder + file + "/")
                print "folder: " + file
                folders_to_search.append(folder + file + "/")
            except ftplib.error_perm:
                print "file: " + folder + file
                files_to_download.append((folder + file, os.path.join(download_loc, file)))

    print files_to_download

        #ftp.retrbinary("RETR " + ftp_cur_dir + file, open(os.path.join(download_loc, file), "wb").write)

for rsync_backup_name in backup_details['rsync']:
    rsync_backup_info = backup_details['rsync'][rsync_backup_name]

    if "store_location" in rsync_backup_info:
        backup_location = rsync_backup_info['store_location']
    else:
        if not os.path.isdir(RSYNC_DUMP_LOC):
            os.mkdir(RSYNC_DUMP_LOC)

        backup_location = os.path.join(RSYNC_DUMP_LOC, rsync_backup_name)

    if not os.path.isdir(backup_location):
        os.mkdir(backup_location)

    rsync_download_loc = os.path.join(backup_location, "current")
    if not os.path.isdir(rsync_download_loc):
        os.mkdir(rsync_download_loc)

    if "rsync_options" in rsync_backup_info:
        rsync_options = rsync_backup_info['rsync_options']
    else:
        rsync_options = "" #by default, no more

    command = "rsync -rthvz --delete " + rsync_options + " -e 'ssh -i " + rsync_backup_info['keyfile'] + "' " +\
        rsync_backup_info['user'] + "@" + rsync_backup_info['host'] + ":" + rsync_backup_info['directory'] + " ."

    print command
    subprocess.call(command, shell=True, cwd=rsync_download_loc) #move to the directory and tell rsync to download to that location

    day_filename = BFM.get_day_filename(rsync_backup_name)
    week_filename = BFM.get_week_filename(rsync_backup_name)
    month_filename = BFM.get_month_filename(rsync_backup_name)

    BFM.create_day_week_month_dirs(backup_location)

    day_backup_loc = BFM.get_day_backup_location(backup_location)

    backups_needed = BFM.backups_need_update(backup_location, rsync_backup_name)

    if backups_needed['day'] or backups_needed['week'] or backups_needed['month']:
        rsync_temp_loc = os.path.join(backup_location, "tmp")
        if not os.path.isdir(rsync_temp_loc):
            os.mkdir(rsync_temp_loc)

        day_file_to_save = os.path.join(rsync_temp_loc, day_filename)

        with tarfile.open(day_file_to_save, "w:gz") as tar:
            for name in os.listdir(rsync_download_loc):
                tar.add(os.path.join(rsync_download_loc, name), name)

        if backups_needed['day']:
            shutil.copy(day_file_to_save, BFM.get_)
        if backups_needed['week']:
            shutil.copy(day_file_to_save, week_backup)
        if backups_needed['month']:
            shutil.copy(day_file_to_save, month_backup)

        os.unlink(day_file_to_save)
    else:
        print "No rsync needed as there are no backups"
