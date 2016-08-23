import time

def get_day_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_day_%Y_%m_%d" + filetype)