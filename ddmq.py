#! /usr/bin/env python

# if python2
from __future__ import print_function
from __future__ import division

import os
import stat
import uuid
import glob
import json
import time
import sys
import fnmatch
import argparse

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


    def json2msg(package):

        # if the package is a string, convert to dict
        if type(package) is str:
            package = json.loads(package)

        new_msg = Message()
        new_msg.__dict__.update(package)
        return new_msg


    def msg2json(self):
        return json.dumps(self.__dict__)

    
    def update(self, package):
        self.__dict__.update(package)

    def __repr__(self):

        text = ""
        for key,val in sorted(self.__dict__.items()):
            text += '{} = {}\n'.format(key,val)
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
        self.create_folder(os.path.join(self.root, queue, 'work'))
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
                self.create_folder(os.path.join(self.root, queue, 'work'))
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
                raise ValueError('Warning, priority set to less than 0 (priority={}). Negative numbers will be sorted in the wrong order when working with messages.'.format(priority))

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

            # construct the path to the file
            msg_filepath = os.path.join(self.root, queue, msg_filename)
            
            # create the new path to the file in the work folder
            msg_work_path = os.path.join(self.root, queue, 'work', '{}.{}'.format(int(time.time()), msg_filename))

            # move to the work folder, adding the current epoch time as a timestamp
            os.rename(msg_filepath, msg_work_path)

            # load the message from the file
            with open(msg_work_path, 'r') as msg_work_handle:
                msg = Message.json2msg(json.load(msg_work_handle))
                restored_messages.append(msg)

            # Tracer()()
            # update the file path
            restored_messages[-1].filename = os.path.join(queue, 'work', '{}.{}'.format(int(time.time()), msg_filename))

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
    
    print(msg)

    Tracer()()


















    sys.exit()
    parser = argparse.ArgumentParser(
        description='Pretends to be git',
        usage='''git <command> [<args>]

The most commonly used git commands are:
list     List queues and number of messages
create    Download objects and refs from another repository
remove
purge
search
''')
    parser.add_argument('command', help='Subcommand to run')
    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args = parser.parse_args(sys.argv[1:2])
    if not hasattr(self, args.command):
        print('Unrecognized command')
        parser.print_help()
        exit(1)
    # use dispatch pattern to invoke method with same name
    getattr(self, args.command)()

    def commit(self):
        parser = argparse.ArgumentParser(
            description='Record changes to the repository')
        # prefixing the argument with -- means it's optional
        parser.add_argument('--amend', action='store_true')
        # now that we're inside a subcommand, ignore the first
        # TWO argvs, ie the command (git) and the subcommand (commit)
        args = parser.parse_args(sys.argv[2:])
        print('Running git commit, amend={}'.format(args.amend))

    def fetch(self):
        parser = argparse.ArgumentParser(
            description='Download objects and refs from another repository')
        # NOT prefixing the argument with -- means it's not optional
        parser.add_argument('repository')
        args = parser.parse_args(sys.argv[2:])
        print('Running git fetch, repository={}'.format(args.repository))


