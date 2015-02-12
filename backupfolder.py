#!/usr/bin/python
import subprocess
import sys

from uploader import Uploader

try:
    BDUSS = open('bduss').read()
except IOError as e:
    print 'No bduss cookie detected, please type it here or save it to file named "bduss"'
    BDUSS = raw_input()
    open('bduss', 'w').write(BDUSS)

class ChunkedFile(object):
    CHUNK_SIZE = 1024 * 1024 * 100

    def __init__(self, upload_worker, file_name):
        self.upload_worker = upload_worker
        self.size = 0
        self.file = None
        self.count = 0
        self.display_name = file_name

    def _uploadAndResetFile(self):
        self.file.close()
        if not self.upload_worker.uploadFile('.bdbackup.tmp', \
            '%s-%.3d' % (self.display_name, self.count)):
            raise IOError('cannot upload some chunks')

        self.count += 1
        self.file = None
        self.size = 0

    def _uploadIfNeeded(self):
        if self.size >= self.CHUNK_SIZE:
            self._uploadAndResetFile()

    def _write(self, data):
        if not self.file:
            self.file = open('.bdbackup.tmp', 'wb')

        self.file.write(data)
        self.size += len(data)

    def write(self, data):
        if not len(data):
            return

        self._write(data)
        self._uploadIfNeeded()

    def close(self):
        if not self.size:
            return

        self._uploadAndResetFile()
        self.upload_worker.doAllJobs()

pipe = subprocess.Popen(['tar', '-zcf', '-', sys.argv[1]], 
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
chunked = ChunkedFile(Uploader(BDUSS, '%2F'), sys.argv[2])

while True:
    data = pipe.stdout.read(1024*1024)

    if not len(data):
        chunked.close()
        break

    chunked.write(data)