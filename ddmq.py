#! /usr/bin/env python

import os
import stat
import uuid
import glob
import json
import time

from IPython.core.debugger import Tracer








class ddmq:




    def __init__(self, root, create=False):

        self.create = create
        self.root = root





    def housekeeping(self):
        return True

    def create_folder(self, path):
        os.makedirs(path)
        st = os.stat(path) # fetch current permissions
        os.chmod(path, st.st_mode | stat.S_IRWXU) # add u+rwx to the folder, leaving g and o as they are

    
    def delete_queue(self, queue):

        return True


    def get_queue_number(self, queue):

        # list all files in queue folder
        try:
            messages = glob.glob(os.path.join(self.root, queue, '*'))
        except OSError as e:

            # try creating the queue if asked to
            if self.create:
                self.create_folder(os.path.join(self.root, queue))
                self.create_folder(os.path.join(self.root, queue, 'processing'))
                messages = glob.glob(os.path.join(self.root, queue, '*'))
            else:
                # raise an error otherwise
                raise OSError(e)

        # init
        max_queue_number = 0

        # for each file
        for msg in sorted(messages):
            
            # get the max queue number at the moment
            current_queue_number = int(os.path.basename(msg).split('.')[1])
            if current_queue_number > max_queue_number:
                max_queue_number = current_queue_number
            
        return max_queue_number+1




    def publish(self, queue=None, message=None, priority=None):

        if not message:
            message = ''

        if not queue:
            raise ValueError('Queue name not specified')

        if not priority:
            priority = 999

        
        # get the next queue number
        queue_number = self.get_queue_number(queue)

        # generate message id
        message_id = '{}.{}.{}{}'.format(priority, queue_number, 'ddmq', uuid.uuid4().hex)

        # Tracer()()
        message_filename = os.path.join(self.root, queue, message_id)
        with open(message_filename, 'w') as message_file:
            message_file.write(json.dumps(message))

        return message_id




    def consume(self, queue, n=1):

        # list all files in queue folder
        messages = sorted(glob.glob(os.path.join(self.root, queue, '*.*.ddmq*')))[:n]

        restored_messages = []
        processing_paths = []
        for msg in messages:

            message_id = os.path.split(msg)[1]
            
            msg_processing_path = os.path.join(self.root, queue, 'processing', '{}.{}'.format(time.time(), message_id))

            # move to the processing folder, adding the current epoch time as a timestamp
            os.rename(msg, msg_processing_path)

            restored_messages.append(json.loads(msg_processing_path))
            processing_paths.append(msg_processing_path)

        if len(restored_messages) == 1:
            return [restored_messages[0], processing_paths[0]]

        return [restored_messages, processing_paths]



# debug
if __name__ == "__main__":
    ddmq = ddmq('testmq', create=True)
    ddmq.publish('testmq','')


