import os
import time


def get_day_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_day_%Y_%m_%d" + filetype)


def get_week_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_week_%Y_%U" + filetype)


def get_month_filename(backup_name, filetype=".tgz"):
    return time.strftime(backup_name + "_month_%Y_%m" + filetype)


def get_day_backup_location(backup_location):
    return os.path.join(backup_location, "day")


def get_week_backup_location(backup_location):
    return os.path.join(backup_location, "week")


def get_month_backup_location(backup_location):
    return os.path.join(backup_location, "month")


def create_day_week_month_dirs(backup_location):
    locations_to_create = [
        get_day_backup_location(backup_location),
        get_week_backup_location(backup_location),
        get_month_backup_location(backup_location)
    ]

    for loc in locations_to_create:
        if not os.path.isdir(loc):
            os.mkdir(loc)


def  backups_need_update(backup_location, backup_name):
    day_backup_loc = get_day_backup_location(backup_location)

    day_backup = os.path.join(day_backup_loc, get_day_filename(backup_name))
    if not os.path.isfile(day_backup):
        copy_day_file = True
    else:
        copy_day_file = False

    week_backup_loc = get_week_backup_location(backup_location)

    week_backup = os.path.join(week_backup_loc, get_week_filename(backup_name))
    if not os.path.isfile(week_backup):
        copy_week_file = True
    else:
        copy_week_file = False

    month_backup_loc = get_month_backup_location(backup_location)

    month_backup = os.path.join(month_backup_loc, get_month_filename(backup_name))
    if not os.path.isfile(month_backup):
        copy_month_file = True
    else:
        copy_month_file = False

    return {
        'day': copy_day_file,
        'week': copy_week_file,
        'month': copy_month_file
    }