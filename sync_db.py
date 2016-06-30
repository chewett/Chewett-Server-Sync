from WookieDb import WookieDb

db = WookieDb.WookieDb(config_file="host_details.json")

dbs = db.show_databases()
for database in dbs:
    db.select_db(database[0])
    print db.show_tables()

    print 'mysqldump.exe --host=localhost --protocol=tcp --user=root --lock-tables=FALSE --compress=TRUE --port=3306 --default-character-set=utf8 --skip-triggers "db" "table"'