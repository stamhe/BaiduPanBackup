#!/usr/bin/python

import sys
import struct
import gzip
import hashlib
import json
import os

BUFFER_SIZE = 1024 * 1024

archive = sys.argv[1]
target = open('%s-stat' % archive, 'rb')
archive_list = target.read().split('\n')
target.close()

print 'Checking integrity of archive files...'
for archive_file in archive_list:
    part = archive_file.split(':')
    index = part[0]
    fn = '%s-%s' % (archive, index)
    md5 = part[1]

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


for archive_file in archive_list:
    fn = '%s-%s' % (archive, archive_file.split(':')[0])

    target = gzip.open(fn, 'rb')
    info_size = struct.unpack('!L', target.read(4))[0]
    info = json.loads(target.read(info_size))

    def mkdir(folder):
        path = './' + folder
        os.makedirs(path)
        print 'Path ', path

    def putFile(filename, offset, data):
        path = './' + filename
        target = open(path, 'ab')
        target.write(data)
        target.close()
        print 'Write', path

    for obj in info:
        filesize = obj['size']
        if filesize == -1:
            mkdir(obj['filename'])
            continue
        else:
            fileoffset = obj['start']
            filename = obj['filename']
            data = target.read(filesize)
            putFile(filename, fileoffset, data)

    target.close()