#!/usr/bin/python

import sys
from uploader import Uploader
from backup import BackupListBuilder, BackupBlob

try:
    BDUSS = open('bduss').read()
except IOError as e:
    print 'No bduss cookie detected, please type it here or save it to file named "bduss"'
    BDUSS = raw_input()
    open('bduss', 'w').write(BDUSS)

backup_name = sys.argv[2]

builder = BackupListBuilder(sys.argv[1])
blobs = [x for x in builder.getBackupBlobs()]
upload_worker = Uploader(BDUSS, '%2F')

i = 0
md5s = []
for blob in blobs:
    md5 = blob.writeToFile('.bdbackup.tmp')
    md5s.append(md5)
    if not upload_worker.uploadFile('.bdbackup.tmp', '%s-%d' % (backup_name, i)):
        print 'Failed to upload some parts'
        sys.exit(1)

target = open('.bdbackup.tmp', 'w')
target.write('\n'.join(md5s))
target.close()
if not upload_worker.uploadFile('.bdbackup.tmp', '%s-stat' % backup_name):
    print 'Failed to upload stat file'
    sys.exit(1)

upload_worker.doAllJobs()