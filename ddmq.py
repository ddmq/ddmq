#! /usr/bin/env python

import os
import stat
import uuid
import glob
import json
import time
import sys
import fnmatch

from IPython.core.debugger import Tracer




class Message:

    def __init__(self, message=None, queue=None, timestamps=None, timeout=None, id=None, priority=None, queue_number=None, filename=None):
        self.message = message
        self.queue = queue
        self.timestamps = timestamps
        self.timeout = timeout
        self.id = id
        self.priority = priority
        self.queue_number = queue_number
        self.filename = filename


    def json2msg(self, package):

        # if the package is a string, convert to dict
        if type(package) is str:
            package = json.loads(package)
        self.message = package['message']
        self.queue = package['queue']
        self.timestamps = package['timestamps']
        self.timeout = package['timeout']
        self.id = package['id']
        self.priority = package['priority']
        self.queue_number = package['queue_number']
        self.filename = package['filename']


    def msg2json(self):

        return json.dumps({'message':self.message, 'queue':self.queue, 'timestamps':self.timestamps, 'timeout':self.timeout, 'id':self.id, 'priority':self.priority, 'queue_number':self.queue_number, 'filename':self.filename})

    def __repr__(self):

        text = ""
        for attr in ['message', 'queue', 'timestamps', 'timeout', 'id', 'priority', 'queue_number', 'filename']:
            text += '{} : {}\n'.format(attr, getattr(msg, attr))
        return text.rstrip()







class ddmq:




    def __init__(self, root, create=False):

        self.create = create
        self.root = root





    def housekeeping(self):
        return True

    def create_folder(self, path):
        # Tracer()()
        os.makedirs(path)
        st = os.stat(path) # fetch current permissions
        os.chmod(path, st.st_mode | stat.S_IRWXU) # add u+rwx to the folder, leaving g and o as they are

    
    def delete_queue(self, queue):
        return True


    def create_queue(self, queue):
        self.create_folder(os.path.join(self.root, queue))
        self.create_folder(os.path.join(self.root, queue, 'processing'))
        return True



    def search_queue(self, queue, query):
        return True



    def delete_message(self, queue, id):
        return True


    def purge_queue(self, queue):
        return True


    def get_message(self, queue, id):
        return True


    def update_message(self, queue, id, update):
        return True


    def get_queue_number(self, queue):
        
        # list all files in queue folder
        try:
            messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
        except FileNotFoundError as e:

            # try creating the queue if asked to
            if self.create:
                self.create_folder(os.path.join(self.root, queue))
                self.create_folder(os.path.join(self.root, queue, 'processing'))
                messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
            else:
                # raise an error otherwise
                raise FileNotFoundError(e)

        # init
        max_queue_number = 0

        # for each file
        for msg in sorted(messages):
            # Tracer()()
            
            # get the max queue number at the moment
            current_queue_number = int(os.path.basename(msg).split('.')[1])
            if current_queue_number > max_queue_number:
                max_queue_number = current_queue_number
            
        return max_queue_number+1




    def publish(self, queue, message=None, priority=None):

        # if no message is given, set it to an empty string
        if not message:
            message = ''

        # if no priority is given, set it to default value
        if not priority:
            priority = 999

        # if it is set, make sure it't not negative
        else:
            if priority < 0:
                raise ValueError('Warning, priority set to less than 0 (priority={}). Negative numbers will be sorted in the wrong order when processing messages.'.format(priority))

        # init a new message object
        msg = Message(message=message, queue=queue, priority=priority, timestamps=[time.time()])

        # get the next queue number
        msg.queue_number = self.get_queue_number(queue)

        # generate message id
        msg.id = uuid.uuid4().hex
        msg.filename = os.path.join(queue, '{}.{}.{}{}'.format(msg.priority, msg.queue_number, 'ddmq', msg.id))

        # Tracer()()

        # write to file
        msg_filepath = os.path.join(self.root, msg.filename)
        with open(msg_filepath, 'w') as message_file:
            message_file.write(msg.msg2json())

        return msg




    def consume(self, queue, n=1):

        # init
        restored_messages = []

        # list all ddmq files in queue folder
        msg_files = sorted(fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*'))[:n]
        # Tracer()()
        
        for msg_filename in msg_files:
            # Tracer()()

            msg = Message()

            # construct the path to the file
            msg_filepath = os.path.join(self.root, queue, msg_filename)
            
            # create the new path to the file in the processing folder
            msg_processing_path = os.path.join(self.root, queue, 'processing', '{}.{}'.format(int(time.time()), msg_filename))

            # move to the processing folder, adding the current epoch time as a timestamp
            os.rename(msg_filepath, msg_processing_path)

            # load the message from the file
            with open(msg_processing_path, 'r') as msg_processing_handle:
                msg.json2msg(json.load(msg_processing_handle))
                restored_messages.append(msg)

            # update the file path
            restored_messages[-1].filename = os.path.join(queue, 'processing', '{}.{}'.format(int(time.time()), msg_filename))

        # return depending on how many messages are collected
        if len(restored_messages) == 0:
            return None
        elif len(restored_messages) == 1:
            return restored_messages[0]
        else:
            return restored_messages



# debug
if __name__ == "__main__":

    root_folder = sys.argv[1]
    ddmq = ddmq(root_folder, create=True)
    msg = ddmq.publish('testmq','testmessage')
    print(msg,'\n')
    # Tracer()()

    msg = ddmq.consume('testmq')
    # Tracer()()
    print(msg)











