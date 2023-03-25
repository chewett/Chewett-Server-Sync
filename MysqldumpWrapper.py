import subprocess
from CommandRunner import run_command
import os
import tempfile


class MysqldumpWrapper:
    '''wrapper for mysqldump to make this slightly easier'''

    def __init__(self, mysqldump_location):
        self.mysql_dump_location = mysqldump_location
        self.mysql_dump_found = self.__check_mysql_dump_exists_here(self.mysql_dump_location)

    def __check_mysql_dump_exists_here(self, path):
        try:
            output = subprocess.check_output([path, "--version"])
            return output
        except Exception as e:
            return False

    def dump_tables_to_file(self, hostname, port_no, user, password, schema, tables, backup_location):
        if self.mysql_dump_found is False:
            raise Exception("Cannot find mysqldump so not executing")

        # set up the cnf file to dump
        tmpfile_file = tempfile.NamedTemporaryFile(delete=False, suffix=".my.cnf", mode="w")
        tmpfile_path = tmpfile_file.name
        tmpfile_file.writelines(["[mysqldump]\r\n", 'password="' + password + '"'])
        tmpfile_file.close() #close the file handle

        table_files = []
        for table in tables:
            table_file = os.path.join(backup_location, table + ".sql")
            command = self.mysql_dump_location + ' --defaults-file=' + tmpfile_path + ' --host=' + hostname + ' --protocol=tcp --user=' + user \
                      + ' --lock-tables=FALSE --no-tablespaces --compress=TRUE --port=' + str(port_no) + ' --default-character-set=utf8 --skip-triggers "' + \
                      schema + '" "' + table + '" > "' + table_file + '"'

            run_command(command)

            table_files.append(table_file)

        os.unlink(tmpfile_path)

        return table_files
