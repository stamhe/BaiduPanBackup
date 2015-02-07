# A script for backing up files to http://pan.baidu.com/

## Usage:

### To backup:

```
python backupfolder.py <folder_to_backup> <backup_name>
```

All files will be uploaded to http://pan.baidu.com/ as <backup_name>-0, <backup_name>-1, ..., <backup_name>-stat

### To restore

Put recover.py into the same directory as backup archive files, and:

```
python recover.py <backup_name>
```