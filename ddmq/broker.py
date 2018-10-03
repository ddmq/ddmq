#! /usr/bin/env python
"""
Defines the broker class which can interact with a ddmq directory.
You define a broker by supplying at least a root directory, for example

>>> b = broker('../temp/ddmq', create=True)
>>> print(b)
create = True
root = ../temp/ddmq

>>> b.publish('queue_name', "Hello World!")
filename = queue_name/999.2.ddmq9d434e370e984ffbabf7455df4acf605
id = 9d434e370e984ffbabf7455df4acf605
message = Hello World!
priority = 999
queue = queue_name
queue_number = 2
requeue = False
timeout = None

>>> msg = b.consume('queue_name')
[filename = 1538484616.999.2.ddmq9d434e370e984ffbabf7455df4acf605
id = 9d434e370e984ffbabf7455df4acf605
message = Hello World!
priority = 999
queue = queue_name
queue_number = 2
requeue = False
timeout = None]

>>> print(msg[0].message)
Hello World!

"""

# if python2
from __future__ import print_function
from __future__ import division
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

try:
  from pathlib import Path
except ImportError:
  from pathlib2 import Path  # python 2 backport


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
import inspect
import re

# import extra modules
import yaml
from message import message

# development
try:
    from IPython.core.debugger import Tracer
except ImportError:
    pass


version = "0.8.3"


class broker:
    """
    Class to interact with messaging queues
    """

    # default queue settings
    settings =  {   'message_timeout': 600, 
                    'cleaned':0,
                    'requeue_prio': 0,
                    }


    def __init__(self, root, create=False, verbose=False, debug=False):
        """
        Initialize a broker object at a specified root directory. If the create flag is set to True it will create the directories needed if they are missing

        Args:
            root:       path to the root directory where the queues are located
            create:     if True, all missing folders will be created without throwing errors
            verbose:    verbose logging to screen
            debug:      even more verbose logging to screen

        Returns:
            None
        """

        # logging
        if verbose:
            log.basicConfig(format="%(levelname)s:\t%(message)s", level=log.INFO)
            log.info("Verbose output.")
        if debug:
            log.basicConfig(format="%(levelname)s:\t%(message)s", level=log.DEBUG)
            log.debug("Debug output.")
        log.debug('Initializing broker object')

        # make sure the root dir is initiated
        if self.check_dir(root, only_conf=True):

            self.create = create
            self.root = root
            self.get_global_settings()

        else:
            # if it should be created
            if create:

                self.create = create
                self.root = root

                # create the root folder and initiate the config file
                self.create_folder(root)
                open(os.path.join(root, 'ddmq.yaml'), 'a').close()

                self.get_global_settings()
            else:
                raise ValueError("Root dir not initiated ({}/ddmq.yaml missing).".format(root))



    def __repr__(self):
        """
        Print the basic options of the broker object

        Args:
            None

        Returns:
            a str that represents the broker object
        """

        log.debug('Printing a broker object')

        # go throguh the variables and collect their names and values
        text = ""
        for key,val in sorted(self.__dict__.items()):
            text += '{} = {}\n'.format(key,val)
        return text.rstrip()
        







 #####  ####### ####### ####### ### #     #  #####   #####  
#     # #          #       #     #  ##    # #     # #     # 
#       #          #       #     #  # #   # #       #       
 #####  #####      #       #     #  #  #  # #  ####  #####  
      # #          #       #     #  #   # # #     #       # 
#     # #          #       #     #  #    ## #     # #     # 
 #####  #######    #       #    ### #     #  #####   #####  
                                                            
    def get_global_settings(self):
        """
        Get the global settings from the config file in the root dir, adding the settings to self.settings
                
        Args:
            None

        Returns:
            None
        """
        log.debug('Updating settings from config file at {}/ddmq.yaml'.format(self.root))
        self.update_settings(os.path.join(self.root, 'ddmq.yaml'))


    def get_queue_settings(self, queue):
        """
        Get settings from a config file from a specified queue dir, , adding the settings to self.settings

        Args:
            queue:  name of the queue to get the settings from

        Returns:
            None
        """
        log.debug('Updating settings from config file at {}/ddmq.yaml'.format(os.path.join(self.root, queue)))
        self.update_settings(os.path.join(self.root, queue, 'ddmq.yaml'))


    def update_settings(self, path):
        """
        Reads the settings from a config file and overrides the settings already in memory

        Args:
            path:   path to the config file to read

        Returns:
            None
        """

        # read the config file and update the settings dict
        with open(path, 'r') as settings_handle:
            try:
                self.settings.update(yaml.load(settings_handle))
            # if the yaml file is empty, load will return None
            except TypeError:
                pass



    def update_settings_file(self, path, package):
        """
        Update the settings in a config file at the specified path
        
        Args:
            path:       path to the config file to be written to
            package:    a dict containging the changes to the config file

        Returns:
            None
        """

        log.debug('Updating config file at {}/ddmq.yaml'.format(path))

        # load the current config file
        try:
            with open(os.path.join(path, 'ddmq.yaml'), 'r') as settings_handle:
                current_settings = yaml.load(settings_handle)
        except FileNotFoundError:
            current_settings = {}

        # if the settings file is empty
        if not current_settings:
            current_settings = {}
            
        # update and write the new
        with open(os.path.join(path, 'ddmq.yaml.intermediate'), 'w') as settings_handle:
            current_settings.update(package)
            settings_handle.write(yaml.dump(current_settings, default_flow_style=False))
        
        # replace the old settings file with the new
        os.rename(os.path.join(path, 'ddmq.yaml.intermediate'), os.path.join(path, 'ddmq.yaml'))





 #####  #       #######    #    #     # ### #     #  #####  
#     # #       #         # #   ##    #  #  ##    # #     # 
#       #       #        #   #  # #   #  #  # #   # #       
#       #       #####   #     # #  #  #  #  #  #  # #  #### 
#       #       #       ####### #   # #  #  #   # # #     # 
#     # #       #       #     # #    ##  #  #    ## #     # 
 #####  ####### ####### #     # #     # ### #     #  #####  

    def clean(self, queue, get_queue_settings=True):
        """
        Clean out expired message from a specified queue
        
        Args:
            queue:                  name of the queue to clean
            get_queue_settings:     if True, the queue specific settings will be loaded before cleaning

        Returns:
            None
        """

        # get the queue settings
        if get_queue_settings:
            self.get_queue_settings(queue)

        # only proceede if enough time as passed since last cleaning
        if not self.settings['cleaned'] < int(time.time())-60:
            return
        
        log.info('Cleaning {}'.format(queue))

        # list all files in queues work folder
        # try:
        messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*')

        # for each message file
        for msg_filename in messages:

            # handle messages that have expired
            msg_expiry_time = int(msg_filename.split('.')[0])
            if msg_expiry_time < int(time.time()):

                # construct the file path
                msg_filepath = os.path.join(self.root, queue, 'work', msg_filename)

                try:
                    # load the message from the file
                    with open(msg_filepath, 'r') as msg_handle:
                        msg = message.json2msg(json.load(msg_handle))
                except (FileNotFoundError, IOError) as e:
                    # race conditions could cause files being removed since the listdir was run
                    print("Warning: while cleaning, message file {} was missing. This could be due to another process operating on the queue at the same time. It should be pretty rare, so if it happens often it could be some other problem causing it.".format(msg_filepath))
                    continue

                # requeue if it should be
                if msg.requeue:

                    # change priority to default value
                    msg.priority = self.settings['requeue_prio']

                    # check if custom requeue prio is set
                    if type(msg.requeue) == int:
                        msg.priority = msg.requeue

                    self.publish(queue=msg.queue, msg_text=msg.message, priority=msg.priority, requeue=msg.requeue, clean=False)

                # then delete the old message file
                os.remove(os.path.join(self.root, queue, 'work', msg_filename))
        
        # update the timestamp for when the queue was last cleaned
        self.update_settings_file(os.path.join(self.root, queue), {'cleaned':int(time.time())})


    def clean_all(self):
        """
        Clean all the queues in the root director
        
        Args:
            None

        Returns:
            None
        """

        log.info('Cleaning all queues')

        # list all queues
        for queue in self.list_queues():

            # clean the queue
            self.clean(queue)






 #####  #     # ####### #     # #######    ####### ######   #####  
#     # #     # #       #     # #          #     # #     # #     # 
#     # #     # #       #     # #          #     # #     # #       
#     # #     # #####   #     # #####      #     # ######   #####  
#   # # #     # #       #     # #          #     # #             # 
#    #  #     # #       #     # #          #     # #       #     # 
 #### #  #####  #######  #####  #######    ####### #        #####  


    def list_queues(self):
        """
        Generate a list of all valid queues (subdirectories with ddmq.yaml files in them) in the root folder
        
        Args:
            None

        Returns:
            a list of names of valid queues
        """

        log.debug('Getting queue list')

        queues = []
        # list all queues
        for queue in sorted(os.listdir(self.root)):

            # skip files
            if not os.path.isdir(os.path.join(self.root, queue)):
                continue

            # save directories that are initiated queues
            if self.check_dir(os.path.join(self.root, queue)):
                queues.append(queue)
        
        return queues


    def get_message_list(self, queue):
        """ 
        Gets a list of all messages in the specified queue
        
        Args:
            queue:  name of the queue to get messages from

        Returns:
            returns 2 lists of file names. The first is the list of all messages still waiting in the queue and the second is a list of all the messages in the queue's work directory
        """

        log.debug('Listing messages in queue {}'.format(queue))
        
        # list all files in queue folder
        # try:
        messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
        # except (FileNotFoundError, OSError) as e:
        #     messages = []

        # list all files in queue work folder
        # try:
        work_messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*')
        # except (FileNotFoundError, OSError) as e:
        #     # the work folder is not really needed to be able to publish messages, and a missing work folder will be handled by the consume function if needed
        #     work_messages = []

        return messages, work_messages


    def delete_queue(self, queue):
        """
        Delete a specified queue
        
        Args:
            queue:  name of the queue to delete

        Returns:
            True if everything goes according to plan
        """

        log.info('Deleting queue {}'.format(queue))

        # gee, don't want to mess this up, do we..
        # remove all ddmq files from the work folder if it exists
        try:
            for msg in fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*'):
                os.remove(os.path.join(self.root, queue, 'work', msg))
            # remove the work dir itself
            os.rmdir(os.path.join(self.root, queue, 'work'))
        except (FileNotFoundError, OSError) as e:
            pass

        # remove all ddmq files in the queue folder
        for msg in fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*'):
            os.remove(os.path.join(self.root, queue, msg))
        
        # remove the queue settings file if existing
        try:
            os.remove(os.path.join(self.root, queue, 'ddmq.yaml'))
            os.remove(os.path.join(self.root, queue, 'ddmq.yaml.intermediate'))
        except (FileNotFoundError, OSError):
            pass

        try:
            os.rmdir(os.path.join(self.root, queue))
        except OSError as e:
            raise OSError('{}   Files created outside of ddmq could be in there, aborting deletion.'.format(e))

        return True


    def create_queue(self, queue):
        """
        Create a specified queue
        
        Args:
            queue:  name of the queue to create

        Returns:
            True if everything goes according to plan
        """

        log.info('Creating queue {}'.format(queue))

        # create the folders a queue needs
        self.create_folder(os.path.join(self.root, queue))
        self.create_folder(os.path.join(self.root, queue, 'work'))
        open(os.path.join(self.root, queue, 'ddmq.yaml'), 'a').close()
        return True


    # def search_queue(self, queue, query):
    #     """
    #     Search the messages of a specified queue for the query term (NOT YET IMPLEMENTED)
        
    #     Args:
    #         queue:  name of the queue to search
    #         query:  query to search for
    #     Returns:
    #         a list of all messages matching to query
    #     """

    #     log.info('Searching {} for "{}"'.format(queue, query))

    #     return True


    # def delete_message(self, path):
    #     """
    #     Delete a specified message (NOT YET IMPLEMENTED)
        
    #     Args:
    #         path:   path to the message to be deleted

    #     Returns:
    #         None
    #     """

    #     log.info('Deleting message {} from {}'.format(id, queue))

    #     return True


    def purge_queue(self, queue):
        """
        Purge the specified queue of all messages, but keep the queue folders and config file
        
        Args:
            queue:  name of the queue to purge

        Returns:
            a list of 2 numbers; the first is how many messages still waiting in the queue were deleted, and the second how many messages in the queues work directory that was deleted
        """

        log.info('Purging {}'.format(queue))

        # init
        removed = 0
        removed_work = 0

        # remove all ddmq files from the work folder if it exists
        try:
            for msg in fnmatch.filter(os.listdir(os.path.join(self.root, queue, 'work')), '*.ddmq*'):
                os.remove(os.path.join(self.root, queue, 'work', msg))
                removed_work += 1
        except (FileNotFoundError, OSError) as e:
            pass

        # remove all ddmq files in the queue folder
        for msg in fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*'):
            os.remove(os.path.join(self.root, queue, msg))
            removed += 1
        
        return removed, removed_work


    # def get_message(self, path):
    #     """
    #     Get a specified message (NOT YET IMPLEMETED)
        
    #     Args:
    #         path:   path to the message to fetch

    #     Returns:
    #         the requested message
    #     """

    #     log.debug('Fetching message {} from {}'.format(id, queue))

    #     return True


    # def update_message(self, path, update):
    #     """
    #     Update a specified message (NOT YET IMPLEMETED)
        
    #     Args:
    #         path:   path to the message to be updated
    #         update: a dict containing the changes to be made

    #     Returns:
    #         None
    #     """

    #     log.debug('Updating message {} in {}'.format(id, queue))

    #     return True






#     # ####### ### #        #####  
#     #    #     #  #       #     # 
#     #    #     #  #       #       
#     #    #     #  #        #####  
#     #    #     #  #             # 
#     #    #     #  #       #     # 
 #####     #    ### #######  #####  

    def check_dir(self, path, only_conf=False):
        """
        Check if the directory contains a ddmq.yaml file to avoid littering non-queue dirs
        
        Args:
            path:       path to the directory to check
            only_conf:  if True, only check if the ddmq.yaml file is present. If False, also check that there is a subdirectory called 'work'

        Returns:
            None
        """
        if os.path.isfile(os.path.join(path, 'ddmq.yaml')):

            # if only the conf file is enough
            if only_conf:
                return True
            
            # check if there is a work dir too
            if os.path.isdir(os.path.join(path, 'work')):
                return True


    def get_queue_number(self, queue):
        """
        Generate the next incremental queue number for a specified queue
        
        Args:
            queue:  name of the queue to generate the queue number for

        Returns:
            an int that is the next queue number in succession
        """
        
        log.debug('Generating next queue number in {}'.format(queue))

        # list all files in queue folder
        try:
            messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
        except (FileNotFoundError, OSError) as e:
            # try creating the queue if asked to
            if self.create:
                self.create_folder(os.path.join(self.root, queue))
                self.create_folder(os.path.join(self.root, queue, 'work'))
                messages = fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*')
            else:
                # raise an error otherwise
                raise FileNotFoundError("Unable to read from the queue folder: {}".format(e))
        
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
        """
        Create a folder at a specified path and make sure the user can rwx the folder
        
        Args:
            path:   path to the directory to be created

        Returns:
            None
        """
        
        log.info('Creating folder: {}'.format(path))

        # create the directory recursivly and set correct permissions
        Path(path).mkdir(exist_ok=self.create)
        st = os.stat(path) # fetch current permissions
        os.chmod(path, st.st_mode | stat.S_IRWXU) # add u+rwx to the folder, leaving g and o as they are






### #     # ####### ####### ######     #     #####  ####### 
 #  ##    #    #    #       #     #   # #   #     #    #    
 #  # #   #    #    #       #     #  #   #  #          #    
 #  #  #  #    #    #####   ######  #     # #          #    
 #  #   # #    #    #       #   #   ####### #          #    
 #  #    ##    #    #       #    #  #     # #     #    #    
### #     #    #    ####### #     # #     #  #####     #    

    def publish(self, queue, msg_text=None, priority=None, clean=True, requeue=False, requeue_prio=None, timeout=None):
        """
        Publish a message to a queue
        
        Args:
            queue:          name of the queue to publish to
            msg_text:       the actual message
            priority:       the priority of the message (default 999). Lower number means higher priority when processing
            clean:          if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just publish the message right away and not bother doing any cleaning first (faster).
            requeue:        if True, the message will be requeud after it expires. If False it will just be deleted.
            requeue_prio:   if set (int), the message will get this priority when requeued. Default is 0, meaning requeued messages will be put first in the queue.
            timeout:        if set (int), will override the global and queue specific default setting for how many seconds a message expires after.

        Returns:
            a copy of the message published
        """

        log.info('Publishing message to {}'.format(queue))

        # get queue specific settings
        self.get_queue_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue, get_queue_settings=False)

        # if no message is given, set it to an empty string
        if not msg_text:
            msg_text = ''

        # check if priority is not set
        if not priority:
            priority = 999
        # if it is set, make sure it't not negative
        else:
            if priority < 0:
                raise ValueError('Warning, priority set to less than 0 (priority={}). Negative numbers will be sorted in the wrong order when working with messages.'.format(priority))

        # check if requeue prio is set and send that value if it is
        if requeue_prio:
            requeue = requeue_prio

        # init a new message object
        msg = message(message=msg_text, queue=queue, priority=priority, timestamps=[time.time()], requeue=requeue, timeout=timeout)

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
        """
        Consume 1 (or more) messages from a specified queue. The consumed messages will be moved to the queues work folder and have the expiry epoch time prepended to the file name.
        
        Args:
            queue:  name of the queue to consume from
            n:      the number (int) of messages to consume
            clean:  if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just consume the message(s) right away and not bother doing any cleaning first (faster).

        Returns:
            a list of the messages that were fetched
        """

        log.info('Consuming {} message(s) from {}'.format(n, queue))

        # get queue specific settings
        self.get_queue_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue, get_queue_settings=False)

        # set default value if missing
        if not n:
            n = 1

        # init
        restored_messages = []
        
        # list all ddmq files in queue folder
        try:
            msg_files = sorted(fnmatch.filter(os.listdir(os.path.join(self.root, queue)), '*.ddmq*'))[:n]
        except (FileNotFoundError, OSError) as e:
            raise FileNotFoundError("Unable to read from the queue folder: {}".format(os.path.join(self.root, queue)))
        
        for msg_filename in msg_files:

            # construct the path to the file
            msg_filepath = os.path.join(self.root, queue, msg_filename)

            try:
                # load the message from the file
                with open(msg_filepath, 'r') as msg_handle:
                    msg = message.json2msg(json.load(msg_handle))
            except (FileNotFoundError, IOError) as e:
                # race conditions could cause files being removed since the listdir was run
                print("Warning: while consuming, the message file {} was missing. This could be due to another process operating on the queue at the same time. It should be pretty rare, so if it happens often it could be some other problem causing it.".format(msg_filepath))
                continue
            
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
        else:
            return restored_messages








 #####  #     # ######      #       ### #     # ####### 
#     # ##   ## #     #     #        #  ##    # #       
#       # # # # #     #     #        #  # #   # #       
#       #  #  # #     #     #        #  #  #  # #####   
#       #     # #     #     #        #  #   # # #       
#     # #     # #     #     #        #  #    ## #       
 #####  #     # ######      ####### ### #     # ####### 


def view(args=None):
    """
    Handle the command-line sub-command view
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """

    if not args:
        parser = argparse.ArgumentParser(
            description='View available queues and number of messages.',
            usage='''{} view [-hfnjvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]'''.format(sys.argv[0])
    )
        # add available options for this sub-command
        parser.add_argument('root', help="the message queue's root folder", type=str)
        parser.add_argument('queue', nargs='?', help="name of specific queue(s) to view", type=str)
        parser.add_argument('-f', action='store_true', help="create the root folder if needed")
        parser.add_argument('-n', action='store_true', help="only print the name of queues (faster)")
        parser.add_argument('--format', nargs='?', help="specify output format (plain, json, yaml)", default='plain', type=str)
        parser.add_argument('-v', action='store_true', help="verbose mode")
        parser.add_argument('-d', action='store_true', help="debug mode")


        # now that we're inside a subcommand, ignore the first two arguments
        args = parser.parse_args(sys.argv[2:])

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # readability
    print_format = args.format
    only_names = args.n
    filter_queues = args.queue


    # get queue names
    queues = brokerObj.list_queues()

    # apply filter if asked to
    if filter_queues:
        all_queues = queues
        queues = []
        filter_queues = filter_queues.split(',')
        for queue in filter_queues:
            if queue in all_queues:
                queues.append(queue)
            else:
                print("Warning: requested queue does not exist ({})".format(queue))

    log.info('Viewing queue(s): {}'.format(', '.join(queues)))
    
    # if number of messages are to be returned as well
    if not only_names:

        # initialize for all queues
        queues = dict((key,[0,0]) for key in queues)

        # fetch the number of messages
        for queue in queues.keys():
            msgs = brokerObj.get_message_list(queue)
            queues[queue] = [len(msgs[0]), len(msgs[1])]

    # print in the requested format
    if print_format not in ['plain', 'json', 'yaml'] and print_format is not None:
        raise ValueError("Unknown format, {}. Valid formats are plain, json and yaml.")

    if print_format == 'json':
        print(json.dumps(queues).rstrip())
        return

    elif print_format == 'yaml':
        print(yaml.dump(queues).rstrip()) # remove the newline, hopefully not important
        return

    else:

        # try using beautifultables if it is installed
        try:
            from beautifultable import BeautifulTable

            table = BeautifulTable()
            if not only_names:
                table.column_headers = ["Queue", "msg in queue", "msg at work"]
            else:
                table.column_headers = ["Queue"]
            for queue in sorted(queues):
                if not only_names:
                    table.append_row([queue, queues[queue][0], queues[queue][1]])
                else:
                    table.append_row([queue])

            # add empty row if there were no queues, otherwise the headers won't print
            if queues == {}:
                if not only_names:
                    table.append_row(['','',''])
                else:
                    table.append_row([''])

        # otherwise fall back to ugly table
        except ImportError:

            table = ""
            if not only_names:
                table += "Queue\t\t\tmsg in queue\tmsg at work\n"
            else:
                table += "Queue\n"
            for queue in sorted(queues):
                if not only_names:
                    table += "{}\t\t\t{}\t\t{}\n".format(queue, queues[queue][0], queues[queue][1])
                else:
                    table += "{}\n".format(queue)
            table = table.rstrip()

        print(str(table))
        return



def create(args=None):
    """
    Handle the command-line sub-command create
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Create queue(s).',
        usage='''{} create [-hfvds] <root> [queue1,queue2,...,queueN]'''.format(sys.argv[0])
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder", type=str)
    parser.add_argument('queue', help="comma-separated names of specific queue(s) to create", type=str)
    parser.add_argument('-f', action='store_true', help="create the root folder if needed")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # readability
    queues = args.queue
    silent = args.s

    log.info('Creating queue(s): {}'.format(', '.join(queues.split(','))))

    # get existing queue names
    existing_queues = brokerObj.list_queues()

    # create the queues
    created_queues = 0
    for queue in queues.split(','):

        # skip names with weird characters in them
        if not bool(re.match('^[a-zA-Z0-9_-]+$', queue)):
            if not silent:
                print("Skipping {}, invalid name".format(queue))
                continue

        # if it already exists
        if queue in existing_queues:
            if not silent:
                print("Already existing: {}".format(queue))

        else:
            try:
                if brokerObj.create_queue(queue):
                    if not silent:
                        print("Created new queue: {}".format(queue))
                    created_queues += 1
                    existing_queues.append(queue)
                
            # if there already is a dir but not a ddmq.yaml file
            except OSError:
                if not silent:
                    print("A directory with the same name as the requested queue ({}) already exists but is not created by ddmq (ddmq.yaml missing). Run the same command again using the (-f) force flag to initiate the directory as a queue.".format(os.path.join(brokerObj.root, queue)))
    
    if not silent and created_queues>1:
        print('Created {} new queues'.format(created_queues))


def delete(args=None):
    """
    Handle the command-line sub-command delete
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Delete queue(s).',
        usage='''{} delete [-hfvds] <root> [queue1,queue2,...,queueN]'''.format(sys.argv[0])
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder", type=str)
    parser.add_argument('queue', help="comma-separated names of specific queue(s) to delete", type=str)
    parser.add_argument('-f', action='store_true', help="create the root folder if needed")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # readability
    queues = args.queue
    silent = args.s

    log.info('Deleting queue(s): {}'.format(', '.join(queues.split(','))))

    # get existing queue names
    existing_queues = brokerObj.list_queues()

    # delete the queues
    deleted_queues = 0
    for queue in queues.split(','):

        # skip names with weird characters in them
        if not bool(re.match('^[a-zA-Z0-9_-]+$', queue)):
            if not silent:
                print("Skipping {}, invalid name".format(queue))
                continue

        # if it doesn't exists
        if queue not in existing_queues:
            if not silent:
                print("Queue not existing: {}".format(queue))

        else:
            if brokerObj.delete_queue(queue):
                if not silent:
                    print("Deleted queue: {}".format(queue))
                deleted_queues += 1
    
    if not silent and deleted_queues>1:
        print('Deleted {} queues'.format(deleted_queues))




def publish(args=None):
    """
    Handle the command-line sub-command publish
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Publish message to a queue.',
        usage='''{} publish [options] <root> <queue> "<message>"'''.format(sys.argv[0])
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder", type=str)
    parser.add_argument('queue', help="name of queue to publish to", type=str)
    parser.add_argument('message', help="message text within quotes", type=str)
    parser.add_argument('-f', action='store_true', help="create the root folder and queue if needed")
    parser.add_argument('-p', '--priority', nargs='?', help="define priority of the message (lower number = higer priority)", type=int)
    parser.add_argument('-t', '--timeout', nargs='?', help="define timeout of the message in seconds", type=int)
    parser.add_argument('-r', '--requeue', action='store_true', help="set to requeue message on fail or timeout. Default priority is 0 (top priority) for requeued messages unless changed by --requeue_prio or config files")
    parser.add_argument('--requeue_prio', help="set custom priority to message when it is requeued. Implies -r even if not explicitly set", type=int)
    parser.add_argument('-C', '--skip_cleaning', action='store_true', help="set to publish the message to the queue without doing cleaning of the queue first")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # make sure the queue exists
    if not brokerObj.check_dir(os.path.join(brokerObj.root, args.queue)):
        # create it if asked to
        if args.f:
            try:
                # skip names with weird characters in them
                if not bool(re.match('^[a-zA-Z0-9_-]+$', args.queue)):
                        sys.exit("Error: invalid queue name ({})".format(args.queue))

                brokerObj.create_queue(args.queue)
            except OSError:
                sys.exit("Unable to write to the specified queue directory ({}).".format(os.path.join(args.root, args.queue)))

            if not args.s:
                print("Created new queue: {}".format(args.queue))
        else:
            sys.exit("The specified queue ({}) does not exist. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(os.path.join(brokerObj.root, args.queue)))  

    if args.skip_cleaning:
        if not args.s:
            print("Skipping queue cleaning.")

    # check if custom requeue prio is set
    requeue = args.requeue
    if args.requeue_prio:
        requeue = True

    # call the publish function with the given arguments
    try:
        msg = brokerObj.publish(queue=args.queue, msg_text=args.message, priority=args.priority, clean=args.skip_cleaning, requeue=requeue, requeue_prio=args.requeue_prio, timeout=args.timeout)
    except IOError:
        sys.exit("Unable to write to the specified queue directory ({}).".format(os.path.join(args.root, args.queue)))

    if not args.s:
        print("Successfully published message:\n\n{}".format(msg))






def consume(args=None):
    """
    Handle the command-line sub-command consume
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Consume message(s) from queue.',
        usage='''{} consume [-hfnCvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]'''.format(sys.argv[0])
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder")
    parser.add_argument('queue', help="comma-separated names of specific queue(s) to delete")
    parser.add_argument('-f', action='store_true', help="create the root folder and queue if needed")
    parser.add_argument('-n', nargs='?', help="the number of messages that will be consumed", type=int)
    parser.add_argument('--format', nargs='?', help="specify output format (plain, json, yaml)", default='json', type=str)
    parser.add_argument('-C', '--skip-cleaning', action='store_false', help="set to publish the message to the queue without doing cleaning of the queue first")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    if args.format:
        if args.format not in ['plain', 'json', 'yaml']:
            raise ValueError("Unknown format, {}. Valid formats are plain, json and yaml.")

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # create the broker object
    try:
        messages = brokerObj.consume(queue=args.queue, n=args.n, clean=args.skip_cleaning)
    except IOError:
        sys.exit("Unable to read/write to the specified queue directory ({}).".format(os.path.join(args.root, args.queue)))


    if not messages:
        print("No more messages in {}".format(args.queue))
        return

    # print the messages in requested format
    for msg in messages:

        # Tracer()()

        if args.format == 'json':
            print(json.dumps(msg.__dict__))
        elif args.format == 'plain':
            print(str(msg))
        elif args.format == 'yaml':
            print(yaml.dump(msg.__dict__).rstrip())
        else:
            # should not happen
            print(json.dumps(msg))




def purge(args=None):
    """
    Handle the command-line sub-command purge
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Purge queue(s).',
        usage='''{} purge [-hfvds] <root> [queue1,queue2,...,queueN]'''.format(sys.argv[0])
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder", type=str)
    parser.add_argument('queue', help="comma-separated names of specific queue(s) to delete", type=str)
    parser.add_argument('-f', action='store_true', help="create the root folder if needed")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    try:
        brokerObj = broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)
    except ValueError:
        sys.exit("The specified root directory ({}) is not initiated. Please run the same command with the (-f) force flag to create and initiate directories as needed.".format(args.root))
    except OSError:
        sys.exit("Unable to write to the specified root directory ({}).".format(args.root))

    # readability
    queues = args.queue
    silent = args.s

    log.info('Purging queue(s): {}'.format(', '.join(queues.split(','))))

    # get existing queue names
    existing_queues = brokerObj.list_queues()

    # purge the queues
    purged_queues = 0
    for queue in queues.split(','):

        # skip names with weird characters in them
        if not bool(re.match('^[a-zA-Z0-9_-]+$', queue)):
            if not silent:
                print("Skipping {}, invalid name".format(queue))
                continue

        # if it doesn't exists
        if queue not in existing_queues:
            if not silent:
                print("Queue does not exist: {}".format(os.path.join(brokerObj.root, queue)))

        else:
            try:
                # purge the queue
                purge_return = brokerObj.purge_queue(queue)
                if purge_return:
                    if not silent:
                        print("Purged queue: {}\t({} messages in queue, {} messages in work)".format(queue, purge_return[0], purge_return[1]))
                    purged_queues += 1
            except OSError:
                print("Error: could not read/write to the queue or work directory ({})".format(os.path.join(brokerObj.root, queue)))
    
    if not silent and purged_queues>1:
        print('Purged {} queues'.format(purged_queues))




def json_payload():
    """
    Handle the command-line sub-command json
    
    Args:
        None

    Returns:
        None
    """
    try:
        # read the payload
        payload = json.loads(sys.argv[2])
    except ValueError:
        sys.exit("Error: unable to load the JSON object")

    # initiate the options with default values
    options = { 'cmd':None,
                'root':None,
                'queue':None,
                'message':None,
                'f':False,
                'v':False,
                'd':False,
                'n':False,
                'format':'plain',
                's':False,
                'priority':None,
                'timeout':None,
                'requeue':False,
                'requeue_prio':None,
                'skip_cleaning':False,
                }

    # apply the payload over the defaults
    options.update(payload)

    # transfer the options to the args object
    for key,val in options.items():
        vars(args)[key] = val

    # use dispatch pattern to invoke method with same name
    eval(args.command)(args=args)






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

    parser = argparse.ArgumentParser(
        description='Command-line interface to Dead Drop Messaging Queue (ddmq).',
        usage='''{0} <command> [<args>]

The available commands are:
view      List queues and number of messages
create    Create a queue
delete    Delete a queue
publish   Publish message to queue
consume   Consume message from queue
purge     Purge all messages from queue
json      Run a command packaged as a JSON object

For more info about the commands, run
{0} <command> -h 

'''.format(sys.argv[0]))
    
    parser.add_argument('command', nargs='?', help='Subcommand to run')
    parser.add_argument('-v', '--version', action='store_true', help='print version')

    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args = parser.parse_args(sys.argv[1:2])

    # check if only version is to be printed
    if args.version:
        print("ddmq version {}".format(version))
        exit(0)

    # if no commandis given
    elif not args.command:
        parser.print_help()
        exit(1)

    # check if there is no command given
    elif args.command not in ['view', 'create', 'delete', 'publish', 'consume', 'purge', 'json']:
        print("Unrecognized command: {}".format(args.command))
        parser.print_help()
        exit(1)


    # rename the json command to avoid name conflict with the json module
    if args.command == 'json':
        args.command = 'json_payload'

    # use dispatch pattern to invoke method with same name
    eval(args.command)()

