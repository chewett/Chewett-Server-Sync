import time

def get_day_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_day_%Y_%m_%d" + filetype)

def get_week_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_week_%Y_%U" + filetype)

def get_month_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_month_%Y_%m" + filetype)