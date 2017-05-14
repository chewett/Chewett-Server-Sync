import json
from DBBackupManager import DBBackupManager
from FTPBackupManager import FTPBackupManager
from RsyncBackupManager import RsyncBackupManager

backup_detail_file = open("backup_details.json", "r")
backup_details = json.load(backup_detail_file)

FTP_DUMP_LOC = "ftps"
RSYNC_DUMP_LOC = "./rsync"

for db_backup_name in backup_details['dbs']:
    db_backup = DBBackupManager(db_backup_name, backup_details['dbs'][db_backup_name])
    try:
        db_backup.backup()
    except Exception as e:
        print "Failed to run backup on", db_backup_name
        print e

for host_backup_name in backup_details['ftp']:
    ftp_backup = FTPBackupManager(host_backup_name, backup_details['ftp'][host_backup_name])
    ftp_backup.backup()

for rsync_backup_name in backup_details['rsync']:
    db_backup = RsyncBackupManager(rsync_backup_name, backup_details['rsync'][rsync_backup_name])
    db_backup.backup()

