import sys
import os
import gzip
import json
import hashlib
import struct

class BackupBlob(object):
    objects = []
    size = 0
    full = False
    BUFFER_SIZE = 1024 * 1024
    readonly = False

    def __init__(self, size=1024*1024*100, max_size=1024*1024*200, read_from_file=None):
        self.expected_size = size
        self.max_size = max_size
        if read_from_file is not None:
            self.readonly = True
            self.read_from_file = read_from_file

    def recover(self):
        if not self.readonly:
            return

        target = gzip.open(self.read_from_file, 'rb')
        len_o = struct.unpack('!L', target.read(4))[0]
        o = target.read(len_o)
        objs = json.loads(o)
        print objs
        for obj in objs:
            filename = obj['name']
            filesize = obj['size']

            print './' + filename
            if filesize == -1:
                os.makedirs('./' + filename)
                continue
            else:
                f = open('./' + filename, 'ab')
                while filesize > 0:
                    data = target.read(self.BUFFER_SIZE)
                    f.write(data)
                    filesize -= len(data)

                f.close()

    def writeToFile(self, filename):
        if self.readonly:
            return

        target = gzip.open(filename, 'wb')
        o = json.dumps(self.objects)
        target.write(struct.pack('!L', len(o)))
        target.write(o)
        for obj in self.objects:
            backup_file = obj['name']
            size = obj['size']

            if size == -1:
                continue
            elif size >0:
                offset = obj['offset']
                f = open(backup_file, 'rb')
                f.seek(0, offset)

                bytes_transfered = 0
                while bytes_transfered < size:
                    data = f.read(self.BUFFER_SIZE)
                    if len(data) == 0:
                        raise IOError('unexpected EOF')

                    target.write(data)
                    bytes_transfered += len(data)

                f.close()
            else:
                raise ValueError('size of backup object is %d!' % size)

        target.close()

        # check md5
        target = open(filename, 'rb')
        md5 = hashlib.md5()

        data = target.read(self.BUFFER_SIZE)
        while len(data) > 0:
            md5.update(data)
            data = target.read(self.BUFFER_SIZE)

        target.close()
        return md5.hexdigest()

    def isFull(self):
        return self.full

    def feedFolder(self, name):
        if self.readonly:
            return

        self.objects.append({
            'name': name,
            'size': -1
            })

    def feedFile(self, filename, filesize, offset=0):
        '''
        returns how many tailing bytes are rejected.
        '''
        if self.readonly:
            return

        if self.full:
            raise ValueError('should never feed a full blob!')

        size_after = self.size + filesize - offset

        if size_after < self.expected_size:
            self.objects.append({
                'name': filename,
                'size': filesize - offset,
                'offset': offset
                })
            self.size = size_after
            return 0
        elif size_after >= self.expected_size and size_after < self.max_size:
            self.objects.append({
                'name': filename,
                'size': filesize - offset,
                'offset': offset
                })
            self.size = size_after
            self.full = True
            return 0
        else:
            accepted = self.expected_size - self.size
            if accepted < 0:
                raise ValueError('accepted byte size is negative!')
            self.object.append({
                'name': filename,
                'size': accepted,
                'offset': offset
                })
            self.full = True
            self.size += accepted
            return filesize - offset - accepted

class BackupListBuilder(object):
    file_list = [] # (name, filesize)
    folder_list = [] # name

    def __init__(self, directory):
        '''
        directory: the folder to backup.
        '''

        if not os.path.isdir(directory):
            raise ValueError('%s is not a directory' % directory)

        self.directory = directory

        filelist = os.listdir(directory)
        for file in filelist:
            full_path = os.path.join(directory, file)

            if os.path.isdir(full_path):
                self.folder_list.append((file, BackupListBuilder(full_path)))
            else:
                filesize = os.path.getsize(full_path)
                self.file_list.append((full_path, filesize))

    def getBackupBlobs(self, blob=None):
        if blob is None:
            blob = BackupBlob()

        blob.feedFolder(self.directory)

        for file in self.file_list:
            filepath = file[0]
            filesize = file[1]
            tailing = blob.feedFile(filepath, filesize)

            while tailing > 0:
                # this blob is full
                yield blob
                blob = BackupBlob()
                tailing = blob.feedFile(filepath, filesize, filesize-tailing)

            if blob.isFull():
                yield blob
                blob = BackupBlob()

        for folder in self.folder_list:
            builder = folder[1]

            sub_blobs = builder.getBackupBlobs(blob)
            # pick up the last one for next pass
            blob = sub_blobs[-1]
            sub_blobs.remove(blob)

            # and yield the others
            for b in sub_blobs:
                yield b

            if blob.isFull():
                yield blob
                blob = BackupBlob()

        yield blob