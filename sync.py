#! /usr/bin/env python3

import os
import json
from DBBackupManager import DBBackupManager
from FTPBackupManager import FTPBackupManager
from RsyncBackupManager import RsyncBackupManager

backup_detail_file = open(os.path.join(os.path.dirname(__file__), "backup_details.json"), "r")
backup_details = json.load(backup_detail_file)

FTP_DUMP_LOC = "ftps"
RSYNC_DUMP_LOC = "./rsync"

if "dbs" in backup_details:
    for db_backup_name in backup_details['dbs']:
        db_backup = DBBackupManager(db_backup_name, backup_details['dbs'][db_backup_name], backup_details['settings'])
        try:
            db_backup.backup()
        except Exception as e:
            print("Failed to run backup on", db_backup_name)
            print(e)
            raise e

if "ftp" in backup_details:
    for host_backup_name in backup_details['ftp']:
        ftp_backup = FTPBackupManager(host_backup_name, backup_details['ftp'][host_backup_name])
        ftp_backup.backup()

if "rsync" in backup_details:
    for rsync_backup_name in backup_details['rsync']:
        db_backup = RsyncBackupManager(rsync_backup_name, backup_details['rsync'][rsync_backup_name], backup_details['settings'])
        db_backup.backup()

