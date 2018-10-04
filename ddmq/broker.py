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
import errno

# import extra modules
import yaml
from message import message

# development
try:
    from IPython.core.debugger import Tracer
except ImportError:
    pass


version = "0.9.5"


class DdmqError(ValueError):
    """
    Helper class to pass custom error messages
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}




class broker:
    """
    Class to interact with messaging queues
    """

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

        # settings
        self.default_settings = {  'message_timeout': 600, # the time in seconds after publishing a message expires
                            'cleaned':0,            # epoch timestamp when a queue was last cleaned
                            'priority':999,         # default message priority when published
                            'requeue':True,         # True if messages that are nacked are to be requeued, False will delete them 
                            'requeue_prio': 0,      # the priority requeued messages will have (0 = top priority)
                                }
        self.global_settings = {}
        self.queue_settings = {}
        
        # make sure the root dir is initiated
        if self.check_dir(root, only_conf=True):

            self.create = create
            self.root = root
            self.global_settings = self.default_settings.copy()
            self.global_settings.update(self.get_config_file())

        else:
            
            # if it should be created
            if create:

                self.create = create
                self.root = root
                self.global_settings = self.default_settings.copy()

                # create the root folder if needed and initiate the config file
                if not os.path.isdir(root):
                    self.create_folder(root)
                open(os.path.join(root, 'ddmq.yaml'), 'w').close()
                open(os.path.join(root, 'ddmq.yaml.example'), 'w').write(yaml.dump(self.default_settings, default_flow_style=False))

            else:
                if not os.path.isdir(root):
                    raise DdmqError("missing")
                else:
                    raise DdmqError("uninitiated")



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
            text += '{} = {}{}'.format(key,val,os.linesep)
        return text.rstrip()
        







 #####  ####### ####### ####### ### #     #  #####   #####  
#     # #          #       #     #  ##    # #     # #     # 
#       #          #       #     #  # #   # #       #       
 #####  #####      #       #     #  #  #  # #  ####  #####  
      # #          #       #     #  #   # # #     #       # 
#     # #          #       #     #  #    ## #     # #     # 
 #####  #######    #       #    ### #     #  #####   #####  
                                                            
    def get_config_file(self, queue=''):
        """
        Get the settings from the config file of a queue or the root dir
                
        Args:
            queue:  if empty, returns the config file from the root folder. If a queue name, will get the config file for that queue

        Returns:
            A dict containing all the settings specified in the config file
        """

        log.debug('Reading config file {}'.format(os.path.join(self.root, queue, 'ddmq.yaml')))

        with open(os.path.join(self.root, queue, 'ddmq.yaml'), 'r') as settings_handle:
            conf = yaml.load(settings_handle)
            if not conf:
                return {}
            return conf




    def get_settings(self, queue):
        """
        Get the settings for the specified queue. Will try to give a cached version first, and if it is the first time the settings are requested it will read the settings from the config file and store the result
                
        Args:
            queue:  name of the queue to get settings for

        Returns:
            None
        """
        
        log.debug('Updating settings from config file {}'.format(os.path.join(self.root, queue, 'ddmq.yaml')))

        try:
            return self.queue_settings[queue]
        except KeyError:

            # must be the first time the queue settings are requested, fetch them from file and store for later
            with open(os.path.join(self.root, queue, 'ddmq.yaml'), 'r') as fh:
                queue_settings = yaml.load(fh)
                self.queue_settings[queue] = self.global_settings.copy()
                self.queue_settings[queue].update(queue_settings)
                return self.queue_settings[queue]





    def update_settings_file(self, queue='', package={}):
        """
        Update the settings in a config file for a specified queue or in the root dir
        
        Args:
            queue:   if empty, change the config in the root folder. If a queue name, will change the config for that queue
            package: a dict containging the changes to the config file

        Returns:
            None
        """

        config_path = os.path.join(self.root, queue, 'ddmq.yaml')
        log.debug('Updating config file {}'.format(config_path))

        # load the current config file
        current_settings = self.get_config_file(queue=queue)

        # update and write the new
        with open(config_path+'.intermediate', 'w') as settings_handle:
            current_settings.update(package)
            settings_handle.write(yaml.dump(current_settings, default_flow_style=False))
        
        # replace the old settings file with the new
        os.rename(config_path+'.intermediate', config_path)





 #####  #       #######    #    #     # ### #     #  #####  
#     # #       #         # #   ##    #  #  ##    # #     # 
#       #       #        #   #  # #   #  #  # #   # #       
#       #       #####   #     # #  #  #  #  #  #  # #  #### 
#       #       #       ####### #   # #  #  #   # # #     # 
#     # #       #       #     # #    ##  #  #    ## #     # 
 #####  ####### ####### #     # #     # ### #     #  #####  

    def clean(self, queue, force=False):
        """
        Clean out expired message from a specified queue
        
        Args:
            queue:                  name of the queue to clean

        Returns:
            True if everything goes according to plan, False if no cleaning was done
        """

        # only proceede if enough time as passed since last cleaning, unless forced
        if not force and (not self.queue_settings[queue]['cleaned'] < int(time.time())-60):
            return False
        
        log.info('Cleaning {}'.format(queue))

        # load the queue's settings
        self.get_settings(queue)

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
                    msg = self.get_message(msg_filepath)
                except (FileNotFoundError, IOError) as e:
                    # race conditions could cause files being removed since the listdir was run
                    print("Warning: while cleaning, message file {} was missing. This could be due to another process operating on the queue at the same time. It should be pretty rare, so if it happens often it could be some other problem causing it.".format(msg_filepath))
                    continue

                # requeue if it should be
                if msg.requeue:

                    # change priority to default value
                    msg.priority = self.queue_settings[queue]['requeue_prio']

                    # check if custom requeue prio is set
                    if type(msg.requeue) == int:
                        msg.priority = msg.requeue

                    # unless the requeue limit has been reached
                    if not msg.requeue_limit or msg.requeue_counter < msg.requeue_limit:
                        
                        # requeue the message
                        self.publish(queue=msg.queue, msg_text=msg.message, priority=msg.priority, requeue=msg.requeue, requeue_counter=msg.requeue_counter+1, requeue_limit=msg.requeue_limit, clean=False)

                # then delete the old message file
                os.remove(os.path.join(self.root, queue, 'work', msg_filename))
        
        # update the timestamp for when the queue was last cleaned
        self.update_settings_file(queue, {'cleaned':int(time.time())})
        return True


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
        with open(os.path.join(self.root, queue, 'ddmq.yaml'), 'w') as fh:
                    fh.write(yaml.dump(self.default_settings, default_flow_style=False))
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


    def get_message(self, path):
        """
        Get a specified message
        
        Args:
            path:   path to the message to fetch

        Returns:
            the requested message
        """

        log.debug('Fetching message {}'.format(path))

        # load the message from the file
        with open(path, 'r') as msg_handle:
             return message.json2msg(json.load(msg_handle))




    def requeue_message(self, path, msg=None):
        """
        Requeue a specified message
        
        Args:
            path:   path to the message to requeue

        Returns:
            True if everything goes according to plan
        """

        log.debug('Requeuing message {}'.format(path))

        

        # load the message from the file
        if not msg:
            msg = self.get_message(path)

        # load the queue's settings
        self.get_settings(msg.queue)

        # requeue if it should be

        # change priority to default value
        msg.priority = self.queue_settings[msg.queue]['requeue_prio']

        # check if custom requeue prio is set
        if type(msg.requeue) == int:
            msg.priority = msg.requeue

        # requeue the message
        self.publish(queue=msg.queue, msg_text=msg.message, priority=msg.priority, requeue=msg.requeue, requeue_counter=msg.requeue_counter+1, requeue_limit=msg.requeue_limit, clean=False)

        # then delete the old message file, assumes the message is consumed and located in the work dir
        os.remove(os.path.join(self.root, msg.queue, 'work', os.path.split(path)[-1]))

        return True


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
        Create a folder at a specified path
        
        Args:
            path:   path to the directory to be created

        Returns:
            None
        """
        
        log.info('Creating folder: {}'.format(path))

        os.makedirs(path)





### #     # ####### ####### ######     #     #####  ####### 
 #  ##    #    #    #       #     #   # #   #     #    #    
 #  # #   #    #    #       #     #  #   #  #          #    
 #  #  #  #    #    #####   ######  #     # #          #    
 #  #   # #    #    #       #   #   ####### #          #    
 #  #    ##    #    #       #    #  #     # #     #    #    
### #     #    #    ####### #     # #     #  #####     #    

    def publish(self, queue, msg_text=None, priority=None, clean=True, requeue=False, requeue_prio=None, timeout=None, requeue_counter=0, requeue_limit=None):
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

        # load the queue's settings
        try:
            self.get_settings(queue)
        except (FileNotFoundError, IOError):
            # create the queue if asked to
            if self.create:
                self.create_queue(queue)
            self.get_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue)

        # if no message is given, set it to an empty string
        if not msg_text:
            msg_text = ''

        # check if priority is not set
        if not priority:
            priority = self.queue_settings[queue]['priority']
        # if it is set, make sure it't not negative
        else:
            if priority < 0:
                raise ValueError('Warning, priority set to less than 0 (priority={}). Negative numbers will be sorted in the wrong order when working with messages.'.format(priority))

        # check if requeue prio is set and send that value if it is
        if requeue_prio:
            requeue = requeue_prio

        # init a new message object
        msg = message(message=msg_text, queue=queue, priority=priority, requeue=requeue, timeout=timeout, requeue_counter=requeue_counter, requeue_limit=requeue_limit)

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

        # load the queue's settings
        try:
            self.get_settings(queue)
        except (FileNotFoundError, IOError):
            # create the queue if asked to
            if self.create:
                self.create_queue(queue)
            self.get_settings(queue)

        # clean the queue unless asked not to
        if clean:
            self.clean(queue)

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
                message_timeout = int(time.time()) + self.queue_settings[queue]['message_timeout']

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


    def nack(self, queue, msg_files, requeue=None, clean=True):
        """
        Negative acknowledgement of message(s)
        
        Args:
            queue:      name of the queue the files are in
            msg_files:  either a single path or a list of paths to message(s) to nack
            requeue:    True will force message(s) to be requeued, False will force messages to be purged, None (default) will leave it up to the message itself if it should be requeued or not
            clean:      if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just ack the message(s) right away and not bother doing any cleaning first (faster).

        Returns:
            True if everything goes according to plan
        """

        # load the queue's settings
        self.get_settings(queue)

        # convert single message to a list if needed
        if type(msg_files) != list:
            msg_files = [msg_files]

        # for each message to process
        nacked = []
        for msg_file in msg_files:

            msg_path = os.path.join(self.root, queue, 'work', msg_file)

            # check if the file exists
            if not os.path.isfile(msg_path):
                print("Warning: message file missing, {}".format(msg_path))
                continue

            # if it should be requeued
            if requeue:
                self.requeue_message(msg_path)

            # if it is up to the message if it should be requeued or not
            elif requeue is None:
                msg = self.get_message(msg_path)
                if msg.requeue:
                    self.requeue_message(msg_path, msg)

            # if not, remove the acknowledged message
            else:
                # assumes the message is consumed and located in the work dir
                try:
                    os.remove(os.path.join(self.root, queue, 'work', msg_file))
                except (FileNotFoundError, OSError) as e:
                    # race conditions could cause files being removed since the listdir was run
                    print("Warning: while nacking, message file {} was missing. This could be due to another process operating on the queue at the same time. It should be pretty rare, so if it happens often it could be some other problem causing it.".format(msg_path))
                    continue
            
            nacked.append(msg_file)
        
        return nacked


    def ack(self, queue, msg_files, requeue=False, clean=True):
        """
        Positive acknowledgement of message(s)
        
        Args:
            queue:      name of the queue the files are in
            msg_files:  either a single path or a list of paths to message(s) to ack
            requeue:    True will force message(s) to be requeued, False (default) will force messages to be purged, None will leave it up to the message itself if it should be requeued or not
            clean:      if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just ack the message(s) right away and not bother doing any cleaning first (faster).

        Returns:
            a list of file names of all messages acknowledged
        """

        # convert single message to a list if needed
        if type(msg_files) != list:
            msg_files = [msg_files]

        # for each message to process
        acked = []
        for msg_file in msg_files:

            msg_path = os.path.join(self.root, queue, 'work', msg_file)

            # check if the file exists
            if not os.path.isfile(msg_path):
                print("Warning: message file missing, {}".format(msg_path))
                continue

            # if it should be requeued
            if requeue:
                self.requeue_message(msg_path)

            # if not, remove the acknowledged message
            else:
                # assumes the message is consumed and located in the work dir
                try:
                    os.remove(os.path.join(self.root, queue, 'work', msg_file))
                except (FileNotFoundError, OSError) as e:
                    # race conditions could cause files being removed since the listdir was run
                    print("Warning: while acking, message file {} was missing. This could be due to another process operating on the queue at the same time. It should be pretty rare, so if it happens often it could be some other problem causing it.".format(msg_path))
                    continue
            
            acked.append(msg_file)
        
        return acked











 #####  #     # ######      #       ### #     # ####### 
#     # ##   ## #     #     #        #  ##    # #       
#       # # # # #     #     #        #  # #   # #       
#       #  #  # #     #     #        #  #  #  # #####   
#       #     # #     #     #        #  #   # # #       
#     # #     # #     #     #        #  #    ## #       
 #####  #     # ######      ####### ### #     # ####### 



def create_broker(root, create=False, verbose=False, debug=False):
    """
    Helper function to create broker objects, printing correct error messages if failed
    
    Args:
        root:       root directory
        create:     True if missing folders should be created
        verbose:    verbose progress reporting
        debug:      even more verbose progress reporting

    Returns:
        a broker object
    """

    # create a broker object
    try:
        brokerObj = broker(root=root, create=create, verbose=verbose, debug=debug)
    except OSError as e:
        
        # if the ddmq.yaml file is missing
        if e.errno == errno.EEXIST:
            sys.exit("The specified root directory ({}) exists but is not initiated. Please run the same command with the (-f) force flag to try to create and initiate directories as needed.".format(root))

        elif e.errno == errno.EACCES:
            sys.exit("Unable to write to the specified root directory ({}).".format(root))

    except DdmqError as e:
        if e[0] == 'uninitiated':
            sys.exit("The specified root directory ({}) exists but is not initiated. Please run the same command with the (-f) force flag to try to create and initiate directories as needed.".format(root))
        
        elif e[0] == 'missing':
            sys.exit("The specified root directory ({}) does not exist. Please run the same command with the (-f) force flag to try to create and initiate directories as needed.".format(root))

    return brokerObj





def view(args=None):
    """
    Handle the command-line sub-command view
    Usage:
    ddmq view [-hfnjvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """

    if not args:
        parser = argparse.ArgumentParser(
            description='View available queues and number of messages.',
            usage='''ddmq view [-hfnjvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]'''
    )
        # add available options for this sub-command
        parser.add_argument('root', help="the message queue's root folder", type=str)
        parser.add_argument('queue', nargs='?', help="name of specific queue(s) to view", type=str)
        parser.add_argument('-f', action='store_true', help="create the root folder if needed")
        parser.add_argument('-n', action='store_true', help="only print the name of queues (faster)")
        parser.add_argument('--format', nargs='?', help="specify output format (plain, json, yaml)", default='default', type=str)
        parser.add_argument('-v', action='store_true', help="verbose mode")
        parser.add_argument('-d', action='store_true', help="debug mode")


        # now that we're inside a subcommand, ignore the first two arguments
        args = parser.parse_args(sys.argv[2:])
    
    # create a broker object
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)


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
    if print_format not in ['plain', 'json', 'yaml', 'default'] and print_format is not None:
        raise ValueError("Unknown format, {}. Valid formats are 'default', 'plain', 'json' and 'yaml'.")

    if print_format == 'json':
        print(json.dumps(queues).rstrip())
        return

    elif print_format == 'yaml':
        print(yaml.dump(queues).rstrip()) # remove the newline, hopefully not important
        return

    else:

        # try using beautifultables if it is installed, otherwise fall back to ugly table
        if print_format == 'default':
            try:
                from beautifultable import BeautifulTable
            except ImportError:
                print_format = 'plain'


        if print_format == 'default':
            table = BeautifulTable()
            if not only_names:
                table.column_headers = ["queue", "msg in queue", "msg at work"]
            else:
                table.column_headers = ["queue"]
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

        # print plain table
        else:
            table = ""

            # just print the table names
            if only_names:
                for queue in sorted(queues):
                    table += queue+os.linesep

            # construct a plain table
            else:
                # add header row
                queues['*queue*'] = ['*msg in queue*', '*msg at work*']
                width_col1 = max([len(x) for x in queues.keys()])
                width_col2 = max([len(str(x[0])) for x in queues.values()])
                width_col3 = max([len(str(x[1])) for x in queues.values()])

                for key,val in sorted(queues.items()):
                    table += "| {0:<{col1}} | {1:<{col2}} | {2:<{col3}} |{3}".format(key, val[0], val[1], os.linesep, col1=width_col1, col2=width_col2, col3=width_col3)

            # remove the last newline
            table = table.rstrip()

        print(str(table))



def create(args=None):
    """
    Handle the command-line sub-command create
    Usage:
    ddmq create [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Create queue(s).',
        usage='''ddmq create [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]'''
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
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

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
    Usage:
    ddmq delete [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Delete queue(s).',
        usage='''ddmq delete [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]'''
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
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

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
    Usage:
    ddmq publish [options] <root> <queue> \"<message>\"
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Publish message to a queue.',
        usage='''ddmq publish [options] <root> <queue> "<message>"'''
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder", type=str)
    parser.add_argument('queue', help="name of queue to publish to", type=str)
    parser.add_argument('message', help="message text within quotes", type=str)
    parser.add_argument('-f', action='store_true', help="create the root folder and queue if needed")
    parser.add_argument('-p', '--priority', nargs='?', help="define priority of the message (lower number = higer priority)", type=int)
    parser.add_argument('-t', '--timeout', nargs='?', help="define timeout of the message in seconds", type=int)
    parser.add_argument('-r', '--requeue', action='store_true', help="set to requeue message on fail or timeout. Default priority for requeued messages is 0 (top priority), unless changed by --requeue_prio or config files")
    parser.add_argument('-l', '--requeue_limit', nargs='?', help="define the number of times the message is allowed to be requeued before being permanently deleted after expiry", type=int)
    parser.add_argument('--requeue_prio', help="set custom priority to message when it is requeued. Implies -r even if not explicitly set", type=int)
    parser.add_argument('-C', '--skip_cleaning', action='store_true', help="set to publish the message to the queue without doing cleaning of the queue first")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

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
        msg = brokerObj.publish(queue=args.queue, msg_text=args.message, priority=args.priority, clean=args.skip_cleaning, requeue=requeue, requeue_prio=args.requeue_prio, timeout=args.timeout, requeue_limit=args.requeue_limit)
    except IOError:
        sys.exit("Unable to write to the specified queue directory ({}).".format(os.path.join(args.root, args.queue)))

    if not args.s:
        print("Successfully published message:{0}{0}{1}".format(os.linesep, msg))






def consume(args=None):
    """
    Handle the command-line sub-command consume
    Usage:
    ddmq consume [-hfnCvd] [--format <plain|json|yaml>] <root> <queue>
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Consume message(s) from queue.',
        usage='''ddmq consume [-hfnCvd] [--format <plain|json|yaml>] <root> <queue>'''
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder")
    parser.add_argument('queue', help="name of queue to consume from")
    parser.add_argument('-f', action='store_true', help="create the root folder and queue if needed")
    parser.add_argument('-n', nargs='?', help="the number of messages that will be consumed", type=int)
    parser.add_argument('--format', nargs='?', help="specify output format (plain, json, yaml)", default='json', type=str)
    parser.add_argument('-C', '--skip-cleaning', action='store_false', help="set to consume the message from the queue without doing cleaning of the queue first")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    if args.format:
        if args.format not in ['plain', 'json', 'yaml']:
            raise ValueError("Unknown format, {}. Valid formats are plain, json and yaml.")

    # create a broker object
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

    # consume the messages
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





def ack(args=None):
    """
    Handle the command-line sub-command ack
    Usage:
    ddmq ack [-hfnCvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Positively acknowledge message(s) from queue.',
        usage='''ddmq ack [-hfnCvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]'''
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder")
    parser.add_argument('queue', help="name of the queue the messages are in")
    parser.add_argument('msg_files', help="comma-separated names of file names of the messages to acknowledge")
    parser.add_argument('-C', '--skip-cleaning', action='store_false', help="set to ack the message without doing cleaning of the queue first")
    parser.add_argument('-r', '--requeue',action='store_true', help="force requeue of the messages")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    brokerObj = create_broker(root=args.root, verbose=args.v, debug=args.d)

    # make the files to a list
    msg_files = args.msg_files.split(',')

    # send the messages to acknowledgement
    acked = brokerObj.ack(args.queue, msg_files, requeue=args.requeue, clean=args.skip_cleaning)
    for msg_file in acked:
        print('acked {}'.format(os.path.join(brokerObj.root, args.queue, 'work', msg_file)))
    
    # print failed acked
    for msg_file in msg_files:
        if msg_file not in acked:
            print('failed ack {}'.format(os.path.join(brokerObj.root, args.queue, 'work', msg_file)))




def nack(args=None):
    """
    Handle the command-line sub-command nack
    Usage:
    ddmq nack [-hfnCvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Negatively acknowledge message(s) from queue.',
        usage='''ddmq nack [-hfnCvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]'''
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder")
    parser.add_argument('queue', help="name of the queue the messages are in")
    parser.add_argument('msg_files', help="comma-separated names of file names of the messages to acknowledge")
    parser.add_argument('-C', '--skip-cleaning', action='store_false', help="set to nack the message without doing cleaning of the queue first")
    parser.add_argument('-r', '--requeue',action='store_true', help="force requeue of the messages")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    brokerObj = create_broker(root=args.root, verbose=args.v, debug=args.d)

    # make the files to a list
    msg_files = args.msg_files.split(',')

    # send the messages to acknowledgement
    nacked = brokerObj.nack(args.queue, msg_files, requeue=args.requeue, clean=args.skip_cleaning)
    for msg_file in nacked:
        print('nacked {}'.format(os.path.join(brokerObj.root, args.queue, 'work', msg_file)))
    
    # print failed acked
    for msg_file in msg_files:
        if msg_file not in nacked:
            print('failed nack {}'.format(os.path.join(brokerObj.root, args.queue, 'work', msg_file)))







def purge(args=None):
    """
    Handle the command-line sub-command purge
    Usage:
    ddmq purge [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Purge queue(s).',
        usage='''ddmq purge [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]'''
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
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

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





def clean(args=None):
    """
    Handle the command-line sub-command clean
    Usage:
    ddmq clean [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Clean queue(s).',
        usage='''ddmq clean [-hfvds] <root> <queue1>[,<queue2>,...,<queueN>]'''
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
    brokerObj = create_broker(root=args.root, create=args.f, verbose=args.v, debug=args.d)

    # readability
    queues = args.queue
    silent = args.s

    log.info('Cleaning queue(s): {}'.format(', '.join(queues.split(','))))

    # get existing queue names
    existing_queues = brokerObj.list_queues()

    # purge the queues
    cleaned_queues = 0
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
                clean_return = brokerObj.clean(queue, force=True)
                if clean_return:
                    if not silent:
                        print("Cleaned queue: {}".format(queue))
                    cleaned_queues += 1
            except OSError:
                print("Error: could not read/write to the queue or work directory ({})".format(os.path.join(brokerObj.root, queue)))
    
    if not silent and cleaned_queues>1:
        print('Cleaned {} queues'.format(cleaned_queues))







def json_payload():
    """
    Handle the command-line sub-command json
    Usage:
    ddmq json \'<json object>\'
    
    Args:
        None

    Returns:
        None
    """

    parser = argparse.ArgumentParser(
        description='Purge queue(s).',
        usage='''ddmq json \'<json object>\''''
        )
    args = parser.parse_args(['json_payload'])

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




# TODO
# def search():
# def remove():
# def modify():






#     #    #    ### #     # 
##   ##   # #    #  ##    # 
# # # #  #   #   #  # #   # 
#  #  # #     #  #  #  #  # 
#     # #######  #  #   # # 
#     # #     #  #  #    ## 
#     # #     # ### #     # 

def main():
    """Run the queue in a command-line mode

Usage:    
ddmq <command> [<args>]

The available commands are:
view      List queues and number of messages
create    Create a queue
delete    Delete a queue
publish   Publish message to queue
consume   Consume message from queue
ack       Positivly acknowledge a message
nack      Negativly acknowledge a message (possibly requeue)
purge     Purge all messages from queue
clean     Clean out expired messages from queue
json      Run a command packaged as a JSON object

For more info about the commands, run
ddmq <command> -h"""

    parser = argparse.ArgumentParser(
        description='Command-line interface to Dead Drop Messaging Queue (ddmq).',
        usage='''ddmq <command> [<args>]

The available commands are:
view      List queues and number of messages
create    Create a queue
delete    Delete a queue
publish   Publish message to queue
consume   Consume message from queue
ack       Positivly acknowledge a message
nack      Negativly acknowledge a message (possibly requeue)
purge     Purge all messages from queue
clean     Clean out expired messages from queue
json      Run a command packaged as a JSON object

For more info about the commands, run
ddmq <command> -h 

''')
    
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
    elif args.command not in ['view', 'create', 'delete', 'publish', 'consume', 'ack', 'nack', 'purge', 'clean', 'json']:
        print("Unrecognized command: {}".format(args.command))
        parser.print_help()
        exit(1)


    # rename the json command to avoid name conflict with the json module
    if args.command == 'json':
        args.command = 'json_payload'

    # use dispatch pattern to invoke method with same name
    eval(args.command)()

# run as command-line tool
if __name__ == "__main__":
    """Run the queue in a command-line mode"""
    main()