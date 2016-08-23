Chewett Site Sync
=================

This allows backing up of servers via FTP and rsync for filesystems
 and mysqldump for databases.
 
##Currently Impleneted


* Downloading of mysql databases via mysql dump
    * Daily/weekly/monthly backups supported
    * Archiving of backups with gzip compression
    * Whitelisting of tables to download via regexes
* FTP sync
    * Vaguely works
* rsync
    * Daily/weekly/monthly backups supported
    * Archiving of backups with gzip compression
    * Specify rsync flags
    
##Todo

* All backup methods
    * Only store X backups per day/week/month
    * zipping instead of gzip

* Database features
    * Blacklisting of tables
    * Syncing over SSH
    * Database dump flags
* FTP syncing
    * Test full features
    * SFTP support
    * Syncing entire directories
    * Possibly rsync style copy via local cache?
* rsync
    * Specify all flags
