#!/usr/bin/python

import sys
import os
import gzip
import json
import struct
import hashlib
import multiprocessing
from subprocess import call

TARBALL_SIZE = 1024 * 1024 * 100 # 100MB
TARBALL_SIZE_MAX = 1024 * 1024 * 200 # 200MB
BUFFER_SIZE = 1024 * 1024
try:
    BDUSS = open('bduss').read()
except IOError as e:
    print 'No bduss cookie detected, please type it here or save it to file named "bduss"'
    BDUSS = raw_input()
    open('bduss', 'w').write(BDUSS)

def makeBackupList(dir):
    def _makeBackupList(dir, job_list=None, current_list=None, current_group_size=0):
        if job_list is None or current_list is None:
            job_list = []
            current_list = []
            current_group_size = 0
            pack_allfile = True

            current_list.append({'filename': dir, 'size': -1})
        else:
            pack_allfile = False

        filelist = os.listdir(dir)
        for file in filelist:
            fn = os.path.join(dir, file)
            if os.path.isdir(fn):
                current_list.append({'filename': fn, 'size': -1})
                (current_list, current_group_size) = \
                    _makeBackupList(fn, job_list, current_list, current_group_size)
            else:
                filesize = os.path.getsize(fn)
                current_group_size += filesize
                current_list.append({'filename': fn, 'size': filesize, 'start': 0})

                if current_group_size >= TARBALL_SIZE:
                    if current_group_size >= TARBALL_SIZE_MAX:
                        # tarball is too big, this file should be split into smaller ones
                        file_size_remaining = current_group_size - TARBALL_SIZE
                        file_offset = filesize - file_size_remaining

                        while file_size_remaining != 0:
                            job_list.append(current_list)
                            current_list = []
                            current_group_size = 0

                            if file_size_remaining < TARBALL_SIZE:
                                pack_size = file_size_remaining
                            else:
                                pack_size = file_size_remaining - TARBALL_SIZE

                            current_list.append({'filename': fn, 'size': pack_size, 'start': file_offset})
                            current_group_size += pack_size

                            file_offset += pack_size
                            file_size_remaining -= pack_size

                    else:
                        job_list.append(current_list)
                        current_list = []
                        current_group_size = 0

        if pack_allfile:
            job_list.append(current_list)
            return job_list
        else:
            return (current_list, current_group_size)

    return _makeBackupList(dir)

def doUploadFile(names, trytime=3):
    filename = names[0]
    upload_name = names[1]

    cmd = []
    cmd.append('curl')
    cmd.append('-F')
    cmd.append('blob=@%s' % filename)
    cmd.append('-o')
    cmd.append('.bdbackup.output.json')
    cmd.append('http://pcs.baidu.com/rest/2.0/pcs/file?method=upload&app_id=250528&\
dir=%2F&ondup=newcopy&BDUSS=' + BDUSS + '&filename=' + ('%s' % backup_name))
    print 'Uploading %d bytes to server' % os.path.getsize(filename)

    if call(cmd) != 0:
        if trytime <= 0:
            os.remove(filename)
            return False

        print 'Failed, retrying...'
        return uploadFile(filename, upload_name, trytime-1)
    else:
        target = open('.bdbackup.output.json', 'rb')
        returned_message = json.loads(target.read())
        target.close()
        os.remove('.bdbackup.output.json')
        os.remove(filename)
        if not returned_message.has_key('md5'):
            print 'Upload failed, cloud returns:\n%s' % json.dumps(returned_message, indent=4)
            return False
        return True

class Uploader(object):
    def __init__(self):
        self.pool = multiprocessing.Pool(processes=5)
        self.jobs = []

    def doAllJobs(self):
        results = self.pool.map(doUploadFile, self.jobs)
        self.jobs = []
        for result in results:
            if result == False:
                return False

        return True

    def uploadFile(self, filename, upload_name):
        myname = '.bdbackup.uploader-%d' % len(self.jobs)
        cmd = ['mv', filename, myname]
        call(cmd)

        self.jobs.append((myname, upload_name))
        if len(self.jobs) == 5:
            return self.doAllJobs()
        else:
            return True

def backup(backup_list, backup_name):
    def dumpAndExit(backup_list):
        print 'Failed to upload some blobs, the backup list has been dumped to bdbackup.json'
        target = open('bdbackup.json', 'w')
        target.write(json.dumps(backup_list))
        target.close()
        sys.exit(1)

    i = 0
    backup_files = []
    worker = Uploader()

    for job in backup_list:
        f = gzip.open('.bdbackup.tmp', 'w')

        info = json.dumps(job)
        f.write(struct.pack('!L', len(info)))
        f.write(info)

        d = 0
        for object in job:
            if object['size'] == -1:
                continue

            target = open(object['filename'], 'rb')
            target.seek(object['start'])
            bytes_transfered = 0
            bytes_to_transfer = object['size']

            d += bytes_to_transfer
            while bytes_transfered < bytes_to_transfer:
                s = bytes_to_transfer - bytes_transfered
                s = BUFFER_SIZE if s > BUFFER_SIZE else s
                data = target.read(s)
                # TODO: what if the file changed while transferring?
                f.write(data)

                bytes_transfered += s

            target.close()

        f.close()

        m = hashlib.md5()
        f = open('.bdbackup.tmp', 'rb')
        data = f.read(BUFFER_SIZE)
        while len(data) != 0:
            m.update(data)
            data = f.read(BUFFER_SIZE)

        f.close()

        backup_files.append('%d:%s' % (i, m.hexdigest()))

        upload_status = worker.uploadFile('.bdbackup.tmp', '%s-%d' % (backup_name, i))

        if not upload_status:
            dumpAndExit(backup_list)

        i += 1

    if not worker.doAllJobs():
        dumpAndExit(backup_list)
        
    target = open('.bdbackup.tmp', 'wb')
    target.write('\n'.join(backup_files))
    target.close()
    upload_status = uploadFile('.bdbackup.tmp', '%s-stat' % backup_name)
    os.remove('.bdbackup.tmp')

    if not upload_status:
        dumpAndExit(backup_list)

backup_list = makeBackupList(sys.argv[1])
backup(backup_list, sys.argv[2])