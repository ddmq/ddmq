#! /usr/bin/env python

# if python2
from __future__ import print_function
from __future__ import division
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

# import standard modules
import os
import stat
import uuid
import glob
import json
import time
import sys
import fnmatch
import argparse
import logging as log

# import extra modules
import yaml
from IPython.core.debugger import Tracer





#     # #######  #####   #####     #     #####  ####### 
##   ## #       #     # #     #   # #   #     # #       
# # # # #       #       #        #   #  #       #       
#  #  # #####    #####   #####  #     # #  #### #####   
#     # #             #       # ####### #     # #       
#     # #       #     # #     # #     # #     # #       
#     # #######  #####   #####  #     #  #####  ####### 

class Message:
    """Class to represent a single message"""


    def __init__(self, message=None, queue=None, timestamps=None, timeout=None, id=None, priority=None, queue_number=None, filename=None, requeue=None):
        """Initialize a message with the given parameters"""

        log.debug('Initializing Message object')

        self.message = message
        self.queue = queue
        self.timestamps = timestamps
        self.timeout = timeout
        self.id = id
        self.priority = priority
        self.queue_number = queue_number
        self.filename = filename
        self.requeue = requeue


    @classmethod
    def json2msg(self, package):
        """Converty a JSON object to a Message object"""

        log.debug('Creating a Message object from a JSON string')

        # if the package is a string, convert to dict
        if type(package) is str:
            package = json.loads(package)

        # create a new empty message and update its values
        new_msg = Message()
        new_msg.__dict__.update(package)
        return new_msg


    def msg2json(self):
        """Convert a Message object to a JSON object"""

        log.debug('Creating a JSON string from a Message object')

        return json.dumps(self.__dict__)

    
    def update(self, package):
        """Update a Message object with the parameters supplied by the package (dict)"""

        log.debug('Updating a Message object')

        self.__dict__.update(package)


    def __repr__(self):
        """Print the values of a Message object"""

        log.debug('Printing a Message object')

        # go throguh the variables and collect their names and values
        text = ""
        for key,val in sorted(self.__dict__.items()):
            text += '{} = {}\n'.format(key,val)
        return text.rstrip()













######  ######  #     #  #####  
#     # #     # ##   ## #     # 
#     # #     # # # # # #     # 
#     # #     # #  #  # #     # 
#     # #     # #     # #   # # 
#     # #     # #     # #    #  
######  ######  #     #  #### # 

class ddmq:
    """Class to interact with messageing queues"""

    # default queue settings
    settings =  {'message_timeout': 600, 'cleaned':0}


    def __init__(self, root, create=False):
        """Initialize a ddmq object at a specified root directory. If the create flag is set to True it will create the directories needed if they are missing"""

        log.debug('Initializing ddmq object')

        self.create = create
        self.root = root
        self.get_global_settings()







 #####  ####### ####### ####### ### #     #  #####   #####  
#     # #          #       #     #  ##    # #     # #     # 
#       #          #       #     #  # #   # #       #       
 #####  #####      #       #     #  #  #  # #  ####  #####  
      # #          #       #     #  #   # # #     #       # 
#     # #          #       #     #  #    ## #     # #     # 
 #####  #######    #       #    ### #     #  #####   #####  
                                                            
    def get_global_settings(self):
        """Get the global settings from the config file in the root dir"""
        log.debug('Updating settings from config file at {}/ddmq.conf'.format(self.root))
        self.update_settings(os.path.join(self.root, 'ddmq.conf'))


    def get_queue_settings(self, queue):
        """Get settings from a config file from a specified queue dir, overriding the global settings from the config file in the root directory"""
        log.debug('Updating settings from config file at {}/ddmq.conf'.format(os.path.join(self.root, queue)))
        self.update_settings(os.path.join(self.root, queue, 'ddmq.conf'))


    def update_settings(self, path):
        """Reads the settings from a config file and overrides the settings already in memory"""

        # read the config file and update the settings dict
        try:
            with open(path, 'r') as settings_handle:
                self.settings.update(yaml.load(settings_handle))
        except FileNotFoundError:
            pass


    def update_settings_file(self, path, package):
        """Update the settings in a config file at the specified path"""

        log.debug('Updating config file at {}/ddmq.conf'.format(path))

        # load the current config file
        try:
            with open(os.path.join(path, 'ddmq.conf'), 'r') as settings_handle:
                current_settings = yaml.load(settings_handle)
        except FileNotFoundError:
            current_settings = {}
            
        # update and write the new
        with open(os.path.join(path, 'ddmq.conf.intermediate'), 'w') as settings_handle:
            current_settings.update(package)
            settings_handle.write(yaml.dump(current_settings, default_flow_style=False))
        
        # replace the old settings file with the new
        os.rename(os.path.join(path, 'ddmq.conf.intermediate'), os.path.join(path, 'ddmq.conf'))





 #####  #       #######    #    #     # ### #     #  #####  
#     # #       #         # #   ##    #  #  ##    # #     # 
#       #       #        #   #  # #   #  #  # #   # #       
#       #       #####   #     # #  #  #  #  #  #  # #  #### 
#       #       #       ####### #   # #  #  #   # # #     # 
#     # #       #       #     # #    ##  #  #    ## #     # 
 #####  ####### ####### #     # #     # ### #     #  #####  

    def clean(self, queue, get_queue_settings=True):
        """Clean out expired message from a specified queue"""

        # get the queue settings
        if get_queue_settings:
            self.get_queue_settings(queue)

        # only proceede if enough time as passed since last cleaning
        if not self.settings['cleaned'] < int(time.time())-60:
            return
        
        log.info('Cleaning {}'.format(queue))

        # list all files in queues work folder
        try:
            messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*')

        except (FileNotFoundError, OSError) as e:
            # try creating the queue if asked to
            if self.create:
                self.create_folder(os.path.join(self.root, queue))
                self.create_folder(os.path.join(self.root, queue, 'work'))
                messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*')
            else:
                # raise an error otherwise
                raise FileNotFoundError(e)

        # for each message file        
        for msg_filename in messages:

            # construct the file path
            msg_filepath = os.path.join(self.root, queue, 'work', msg_filename)

            # load the message from the file
            with open(msg_filepath, 'r') as msg_handle:
                msg = Message.json2msg(json.load(msg_handle))

            # handle messages that have expired
            msg_expiry_time = int(msg_filename.split('.')[0])
            if msg_expiry_time < int(time.time()):

                # requeue if it should be
                if msg.requeue:
                    self.publish(queue=msg.queue, message=msg.message, priority=msg.priority, requeue=msg.requeue, clean=False)

                # then delete the old message file
                os.remove(os.path.join(self.root, queue, 'work', msg_filename))
        
        # update the timestamp for when the queue was last cleaned
        self.update_settings_file(os.path.join(self.root, queue), {'cleaned':int(time.time())})


    def clean_all(self):
        """Clean all the queues in the root director"""

        log.info('Cleaning all queues')

        # list all queues
        for queue in sorted(os.listdir(self.root)):

            # skip files
            if not os.path.isdir(os.path.join(self.root, queue)):
                continue

            # clean the queue
            self.clean(queue)






 #####  #     # ####### #     # #######    ####### ######   #####  
#     # #     # #       #     # #          #     # #     # #     # 
#     # #     # #       #     # #          #     # #     # #       
#     # #     # #####   #     # #####      #     # ######   #####  
#   # # #     # #       #     # #          #     # #             # 
#    #  #     # #       #     # #          #     # #       #     # 
 #### #  #####  #######  #####  #######    ####### #        #####  

    def delete_queue(self, queue):
        """Delete a specified queue"""

        log.info('Deleting queue {}'.format(queue))
        return True


    def create_queue(self, queue):
        """Create a specified queue"""

        log.info('Creating queue {}'.format(queue))

        # create the folders a queue needs
        self.create_folder(os.path.join(self.root, queue))
        self.create_folder(os.path.join(self.root, queue, 'work'))
        return True


    def search_queue(self, queue, query):
        """Search the messages of a specified queue for the query term"""

        log.info('Searching {} for "{}"'.format(queue, query))

        return True


    def delete_message(self, queue, id):
        """Delete a specified message"""

        log.info('Deleting message {} from {}'.format(id, queue))

        return True


    def purge_queue(self, queue):
        """Purge the specified queue"""

        log.info('Purging {}'.format(queue))

        return True


    def get_message(self, queue, id):
        """Get a specified message"""

        log.debug('Fetching message {} from {}'.format(id, queue))

        return True


    def update_message(self, queue, id, update):
        """Update a specified message"""

        log.debug('Updating message {} in {}'.format(id, queue))

        return True






#     # ####### ### #        #####  
#     #    #     #  #       #     # 
#     #    #     #  #       #       
#     #    #     #  #        #####  
#     #    #     #  #             # 
#     #    #     #  #       #     # 
 #####     #    ### #######  #####  

    def get_queue_number(self, queue):
        """Generate the next incremental queue number for a specified queue"""
        
        log.debug('Generating next queue number in {}'.format(queue))

        # list all files in queue folder
        messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
        
        # init
        max_queue_number = 0

        # for each file
        for msg in sorted(messages):

            # get the max queue number at the moment
            current_queue_number = int(os.path.basename(msg).split('.')[1])
            if current_queue_number > max_queue_number:
                max_queue_number = current_queue_number
            
        return max_queue_number+1


    def create_folder(self, path):
        """Create a folder at a specified path and make sure the user can rwx the folder"""
        
        log.info('Creating folder: {}'.format(path))

        # create the directory recursivly and set correct permissions
        os.makedirs(path)
        st = os.stat(path) # fetch current permissions
        os.chmod(path, st.st_mode | stat.S_IRWXU) # add u+rwx to the folder, leaving g and o as they are






### #     # ####### ####### ######     #     #####  ####### 
 #  ##    #    #    #       #     #   # #   #     #    #    
 #  # #   #    #    #       #     #  #   #  #          #    
 #  #  #  #    #    #####   ######  #     # #          #    
 #  #   # #    #    #       #   #   ####### #          #    
 #  #    ##    #    #       #    #  #     # #     #    #    
### #     #    #    ####### #     # #     #  #####     #    

    def publish(self, queue, message=None, priority=999, clean=True, requeue=False):
        """Publish a message to a queue"""

        log.info('Publishing message to {}'.format(queue))

        # get queue specific settings
        self.get_queue_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue, get_queue_settings=False)

        # if no message is given, set it to an empty string
        if not message:
            message = ''

        # if it is set, make sure it't not negative
        else:
            if priority < 0:
                raise ValueError('Warning, priority set to less than 0 (priority={}). Negative numbers will be sorted in the wrong order when working with messages.'.format(priority))

        # init a new message object
        msg = Message(message=message, queue=queue, priority=priority, timestamps=[time.time()], requeue=requeue)

        # get the next queue number
        msg.queue_number = self.get_queue_number(queue)

        # generate message id
        msg.id = uuid.uuid4().hex
        msg.filename = os.path.join(queue, '{}.{}.ddmq{}'.format(msg.priority, msg.queue_number, msg.id))

        # write the message to file
        msg_filepath = os.path.join(self.root, msg.filename)
        with open(msg_filepath, 'w') as message_file:
            message_file.write(msg.msg2json())

        return msg




    def consume(self, queue, n=1, clean=True):
        """Consume 1 (or more) messages from a specified queue"""

        log.info('Consuming {} message(s) from {}'.format(n, queue))

        # get queue specific settings
        self.get_queue_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue, get_queue_settings=False)

        # init
        restored_messages = []

        # list all ddmq files in queue folder
        msg_files = sorted(fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*'))[:n]
        
        for msg_filename in msg_files:

            # construct the path to the file
            msg_filepath = os.path.join(self.root, queue, msg_filename)

            # load the message from the file
            with open(msg_filepath, 'r') as msg_handle:
                msg = Message.json2msg(json.load(msg_handle))
            
            # create the new path to the file in the work folder
            if msg.timeout:
                message_timeout = int(time.time()) + msg.timeout
            else:    
                message_timeout = int(time.time()) + self.settings['message_timeout']

            # move to the work folder, adding the message expiry time to the file name
            msg_work_path = os.path.join(self.root, queue, 'work', '{}.{}'.format(message_timeout, msg_filename))
            os.rename(msg_filepath, msg_work_path)
            msg.filename = os.path.split(msg_work_path)[1]

            # save msg
            restored_messages.append(msg)


        # return depending on how many messages are collected
        if len(restored_messages) == 0:
            return None
        elif len(restored_messages) == 1:
            return restored_messages[0]
        else:
            return restored_messages






#     #    #    ### #     # 
##   ##   # #    #  ##    # 
# # # #  #   #   #  # #   # 
#  #  # #     #  #  #  #  # 
#     # #######  #  #   # # 
#     # #     #  #  #    ## 
#     # #     # ### #     # 

# debug
if __name__ == "__main__":
    """Run the queue in a command-line mode"""

    verbose = True
    if verbose:
        log.basicConfig(format="%(levelname)s:\t%(message)s", level=log.DEBUG)
        log.debug("Verbose output.")
    else:
        log.basicConfig(format="%(levelname)s:\t%(message)s")

    root_folder = sys.argv[1]
    ddmq = ddmq(root_folder, create=True)
    msg = ddmq.publish('testmq','testmessage', requeue=True)
    print(msg,'\n')
    # Tracer()()

    msg = ddmq.consume('testmq', 5)
    
    print(msg)

    # Tracer()()


















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


