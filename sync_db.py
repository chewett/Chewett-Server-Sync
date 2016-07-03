from WookieDb import WookieDb
import json
import os
import shutil
import subprocess
import time
import tarfile
import uuid

backup_detail_file = open("backup_details.json", "r")
backup_details = json.load(backup_detail_file)

MYSQL_DUMP_LOC = '"C:\\Program Files\\MySQL\\MySQL Server 5.7\\bin\\mysqldump.exe"'
DB_DUMP_LOC = "dbs"

print backup_details

for db_backup_name in backup_details['dbs']:
    print db_backup_name
    host_backup_info = backup_details['dbs'][db_backup_name]

    my_cnf_filename = db_backup_name + '.my.cnf'
    my_cnf_file = open(my_cnf_filename, 'w')
    my_cnf_file.writelines(["[mysqldump]\r\n", 'password="' + host_backup_info['password'] + '"'])
    my_cnf_file.close()

    db = WookieDb.WookieDb(host=host_backup_info['host'], user=host_backup_info['user'], password=host_backup_info['password'], db=host_backup_info['schema'])
    tables = db.show_tables()

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

    for table in tables:
        command = MYSQL_DUMP_LOC + ' --defaults-file='+ my_cnf_filename +' --host=' + host_backup_info['host'] +' --protocol=tcp --user=' + host_backup_info['user'] + ' --lock-tables=FALSE --compress=TRUE --port=3306 --default-character-set=utf8 --skip-triggers "'+ host_backup_info['schema']+'" "'+table[0]+'" > "' + os.path.join(download_loc, table[0] + ".sql")+ '"'
        print command
        subprocess.call(command, shell=True)

    day_filename = time.strftime(db_backup_name + "_%Y_%m_%d.tgz")
    week_filename = time.strftime(db_backup_name + "_%Y_%U.tgz")
    day_file_to_save = os.path.join(db_download_loc, day_filename)

    with tarfile.open(day_file_to_save, "w:gz") as tar:
        for name in os.listdir(download_loc):
            tar.add(os.path.join(download_loc,name))

    day_backup_loc = os.path.join(backup_location, "day")
    if not os.path.isdir(day_backup_loc):
        os.mkdir(day_backup_loc)

    day_backup = os.path.join(day_backup_loc, day_filename)
    if not os.path.isfile(day_backup):
        shutil.copy(day_file_to_save, day_backup)

    week_backup_loc = os.path.join(backup_location, "week")
    if not os.path.isdir(week_backup_loc):
        os.mkdir(week_backup_loc)

    week_backup = os.path.join(week_backup_loc, week_filename)
    if not os.path.isfile(week_backup):
        shutil.copy(day_file_to_save, week_backup)

    shutil.rmtree(download_loc)
    os.unlink(day_file_to_save)
    os.unlink(my_cnf_filename)


exit()

db = WookieDb.WookieDb(config_file="host_details.json")

dbs = db.show_databases()
for database in dbs:
    db.select_db(database[0])
    print db.show_tables()
