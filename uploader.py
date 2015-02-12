import json
import multiprocessing
import os
import threading

from subprocess import call

def doUploadFile(parameters):
    '''
    upload function called by the worker, parameters is a tuple consists of
    four strings: local file name, file name of that uploaded to remote,
    bduss, remote directory.
    '''

    file_name = parameters[0]
    upload_name = parameters[1]

    bduss = parameters[2]
    remote_dir = parameters[3]

    thread_id = str(upload_name)

    cmd = ['curl']

    cmd.append('-F')
    cmd.append('blob=@%s' % file_name)

    cmd.append('-o')
    json_filename = '.bdbackup.output.%s.json' % thread_id
    cmd.append(json_filename)

    cmd.append('--retry')
    cmd.append('3')

    cmd.append('http://pcs.baidu.com/rest/2.0/pcs/file?method=upload&app_id=250528&\
dir=%s&ondup=newcopy&BDUSS=%s&filename=%s' % (remote_dir, bduss, upload_name))

    print 'Uploading %d bytes to server' % os.path.getsize(file_name)

    ret = call(cmd)
    os.remove(file_name)

    if ret != 0:
        return False
    else:
        target = open(json_filename, 'rb')
        returned_message = json.loads(target.read())
        target.close()
        os.remove(json_filename)
        if not returned_message.has_key('md5'):
            print 'Upload failed, cloud returns:\n%s' % json.dumps(returned_message, indent=4)
            return False
        return True

class Uploader(object):
    WORKER_COUNT = 5

    def __init__(self, bduss, remote_dir):
        self.pool = multiprocessing.Pool(processes=self.WORKER_COUNT)
        self.jobs = []
        self.bduss = bduss
        self.remote_dir = remote_dir

    def doAllJobs(self):
        if len(self.jobs) == 0:
            return True

        results = self.pool.map(doUploadFile, self.jobs)
        self.jobs = []
        for result in results:
            if result == False:
                return False

        return True

    def uploadFile(self, filename, upload_name):
        filename_in_worker = '.bdbackup.uploader-%d' % len(self.jobs)
        os.rename(filename, filename_in_worker)

        self.jobs.append((filename_in_worker, upload_name, self.bduss, self.remote_dir))
        if len(self.jobs) == self.WORKER_COUNT:
            return self.doAllJobs()
        else:
            return True