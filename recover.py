#!/usr/bin/python

import sys
import struct
import gzip
import hashlib
import json
import os

from backup import BackupBlob

BUFFER_SIZE = 1024 * 1024

archive = sys.argv[1]
target = open('%s-stat' % archive, 'rb')
archive_list = target.read().split('\n')
target.close()

print 'Checking integrity of archive files...'
for i in range(0, len(archive_list)):
    fn = '%s-%d' % (archive, i)
    md5 = archive_list[i]

    m = hashlib.md5()
    f = open(fn, 'rb')
    data = f.read(BUFFER_SIZE)
    while len(data) != 0:
        m.update(data)
        data = f.read(BUFFER_SIZE)

    f.close()

    if m.hexdigest() != md5:
        print 'file: %s is broken, exit.' % fn
        sys.exit(1)

print 'All files seems good.'


for i in range(0, len(archive_list)):
    fn = '%s-%d' % (archive, i)

    blob = BackupBlob(read_from_file=fn)
    blob.recover()