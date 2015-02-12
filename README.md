# A script for backing up files to http://pan.baidu.com/

## Usage:

### To backup:

```
python backupfolder.py <folder_to_backup> <backup_name>
```

All files will be uploaded to http://pan.baidu.com/ as ```<backup_name>-000```, ```<backup_name>-001```, ...

### To restore

The backup files are splited .tar.gz files, just concatenate them and unpack:

```
cat $(ls <backup_name>-*) | tar -xvf -
```