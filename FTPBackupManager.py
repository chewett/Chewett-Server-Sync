import os
import shutil
import uuid
from ftplib import FTP
import ftplib


class FTPBackupManager:
    ''' THIS DOESNT REALLY WORK ATM '''

    FTP_DUMP_LOC = "ftp"

    def __init__(self, backup_name, backup_config):
        print(backup_name)
        self.backup_name = backup_name
        self.backup_config = backup_config

    def backup(self):

        if not os.path.isdir(FTPBackupManager.FTP_DUMP_LOC):
            os.mkdir(FTPBackupManager.FTP_DUMP_LOC)

        backup_location = os.path.join(FTPBackupManager.FTP_DUMP_LOC, self.backup_name)
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

        ftp = FTP(self.backup_config['host'], self.backup_config['user'], self.backup_config['password'])
        # ftp.login() # FIXME: Look at why I dont want this

        ftp_root_dir = self.backup_config["directory"]

        folders_to_search = [ftp_root_dir]
        files_to_download = []

        while len(folders_to_search) > 0:
            folder = folders_to_search[0]
            folders_to_search.remove(folder)

            ftp.cwd(folder)
            filelist = ftp.nlst()
            print(filelist)

            for file in filelist:
                try:
                    # this will check if file is folder:
                    ftp.cwd(folder + file + "/")
                    print("folder: " + file)
                    folders_to_search.append(folder + file + "/")
                except ftplib.error_perm:
                    print("file: " + folder + file)
                    files_to_download.append((folder + file, os.path.join(download_loc, file)))

        print(files_to_download)

        # ftp.retrbinary("RETR " + ftp_cur_dir + file, open(os.path.join(download_loc, file), "wb").write)

