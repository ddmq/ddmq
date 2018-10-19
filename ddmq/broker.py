#! /usr/bin/env python
"""
Defines the broker class which can interact with a ddmq directory.
You define a broker by supplying at least a root directory, for example

>>> import ddmq

>>> b = ddmq.broker('../temp/ddmq', create=True)
>>> print(b)
create = True
default_settings = {'priority': 999, 'requeue': True, 'requeue_prio': 0, 'message_timeout': 600, 'cleaned': 0}
global_settings = {'priority': 999, 'requeue': True, 'requeue_prio': 0, 'message_timeout': 600, 'cleaned': 0}
queue_settings = {}
root = ../temp/ddmq


>>> b.publish('queue_name', "Hello World!")
filename = queue_name/999.1.ddmq89723438b9d0403c91943f4ffaf8ba35
id = 89723438b9d0403c91943f4ffaf8ba35
message = Hello World!
priority = 999
queue = queue_name
queue_number = 1
requeue = False
requeue_counter = 0
requeue_limit = None
timeout = None


>>> msg = b.consume('queue_name')
filename = 1539702458.999.1.ddmq89723438b9d0403c91943f4ffaf8ba35
id = 89723438b9d0403c91943f4ffaf8ba35
message = Hello World!
priority = 999
queue = queue_name
queue_number = 1
requeue = False
requeue_counter = 0
requeue_limit = None
timeout = None


>>> print(msg.message)
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
import uuid
import json
import time
import fnmatch
import logging as log
import re

# import extra modules
import yaml
try:
    from .message import message
except ValueError:
    from message import message

# from IPython.core.debugger import Tracer
# Tracer()()

version = "0.9.10"


class DdmqError(Exception):
    """
    Helper class to raise custom errors
    """
    def __init__(self, message, error):
        """
        Initialize a DdmqError object

        Args:
            message:    error message
            error:      error code

        Returns:
            None
        """
        self.message = message
        self.error = error




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
                    raise DdmqError("Root folder missing!", "missing")
                else:
                    raise DdmqError("Root folder uninitiated!", "uninitiated")



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


    def delete_message(self, path):
        """
        Delete a specified message
        
        Args:
            path:   path to the message, or a message object, to be deleted

        Returns:
            None
        """

        # check if the path is a message object
        if path.__class__ == message:

            # check if the message has been consumed already
            match = re.search('^\d+\.\d+\.\d+\.ddmq[a-zA-Z0-9]+$', path.filename)
            if match:
                path = os.path.join(self.root, path.queue, 'work', path.filename)

            # check if the message has not yet been consumed
            match = re.search('^\d+\.\d+\.ddmq[a-zA-Z0-9]+$', path.filename)
            if match:
                path = os.path.join(self.root, path.queue, 'work', path.filename)


        log.info('Deleting message {}'.format(path))

        # make sure the path follows the ddmq naming scheme
        match = re.search('^.+(\d+\.)?\d+\.\d+\.ddmq[a-zA-Z0-9]+$', path)
        if not match:
            raise ValueError('The specified path ({}) does not look like a ddmq message file name'.format(path))

        else:
            # delete the message
            os.remove(path)

            return True


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
        msg.filename = os.path.join('{}.{}.ddmq{}'.format(msg.priority, msg.queue_number, msg.id))

        # write the message to file
        msg_filepath = os.path.join(self.root, queue, msg.filename)
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
            a single message object if n=1 (default), or a list of the messages that were fetched if n > 1
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
        
        # if only one message was requested
        elif n == 1:
            return restored_messages[0]
        
        # if more than one was requested, return a list of messages regardless of its length (even if only 1)
        else:
            return restored_messages


    def nack(self, queue, msg_files=None, requeue=None, clean=True):
        """
        Negative acknowledgement of message(s)
        
        Args:
            queue:      name of the queue the files are in, or the message object to be nacked
            msg_files:  either a single path or a list of paths to message(s) to nack
            requeue:    True will force message(s) to be requeued, False will force messages to be purged, None (default) will leave it up to the message itself if it should be requeued or not
            clean:      if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just ack the message(s) right away and not bother doing any cleaning first (faster).

        Returns:
            True if everything goes according to plan
        """

        # check if the queue is actually a message object
        if queue.__class__ == message:

            # let the options in this function call override the ones in the message
            if not requeue:
                requeue = queue.requeue

            # extract message info
            msg_files = queue.filename
            queue = queue.queue


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


    def ack(self, queue, msg_files=None, requeue=None, clean=True):
        """
        Positive acknowledgement of message(s)
        
        Args:
            queue:      name of the queue the files are in, or the message object to be acked
            msg_files:  either a single path or a list of paths to message(s) to ack
            requeue:    True will force message(s) to be requeued, False will force messages to be purged, None (default) will leave it up to the message itself if it should be requeued or not
            clean:      if True, the client will first clean out any expired messages from the queue's work directory. If False, the client will just ack the message(s) right away and not bother doing any cleaning first (faster).

        Returns:
            a list of file names of all messages acknowledged
        """

        # check if the queue is actually a message object
        if queue.__class__ == message:

            # let the options in this function call override the ones in the message
            if not requeue:
                requeue = queue.requeue

            # extract message info
            msg_files = queue.filename
            queue = queue.queue


        # check that there are message files
        if not msg_files:
            raise ValueError('Message files list is empty.')

        # convert single message to a list if needed
        if type(msg_files) != list:
            msg_files = [msg_files]

        # for each message to process
        acked = []
        for msg_file in msg_files:

            # construct the path to the message file
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





    def version(self):
        """
        Get package version
        
        Args:
            None

        Returns:
            the package version
        """
        return version








