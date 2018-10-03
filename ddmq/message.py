#! /usr/bin/env python
"""
Defines the message class which represents a single message. This class is
primarily to be used by the methods in the broker class.
You define a message by supplying selected arguments, for example

>>> msg = message(queue='queue_name', message='Hello World!')
>>> print(msg)
filename = None
id = None
message = Hello World!
priority = None
queue = queue_name
queue_number = None
requeue = None
timeout = None

"""

import json
import logging as log
import os

class message:
    """
    Class to represent a single message
    """


    def __init__(self, queue=None, message=None, timeout=None, id=None, priority=None, queue_number=None, filename=None, requeue=None, requeue_counter=None, requeue_limit=None):
        """
        Initialize a message with the given parameters
        
        Args:
            queue:          name of the queue the message belongs to
            message:        the message text itself
            timeout:        a custom timeout limit to be used when processing the message, in seconds
            id:             randomly generated uuid for the message
            priority:       a custom priority to be used when processing the message
            queue_number:   the number in the queue the message has. This number determins the order of messages with the same priority level
            filename:       file name of the file containing this message
            requeue:        if True, the message will be requeued with default priority after it expires. If set to an int, that will be used as a custom requeuing priority
            counter         counts the number of times the message has been placed in queue. A list where the first number tells how many times the message has been processed and the second number defines how many times it should be processed at most (the default None means infinite)

        Returns:
            None
        """

        log.debug('Initializing message object')

        self.message = message
        self.queue = queue
        self.timeout = timeout
        self.id = id
        self.priority = priority
        self.queue_number = queue_number
        self.filename = filename
        self.requeue = requeue
        self.requeue_counter = requeue_counter
        self.requeue_limit = requeue_limit


    @classmethod
    def json2msg(self, package):
        """Converty a JSON object to a message object"""

        log.debug('Creating a message object from a JSON string')

        # if the package is a string, convert to dict
        if type(package) is str:
            package = json.loads(package)

        # create a new empty message and update its values
        new_msg = message()
        new_msg.__dict__.update(package)
        return new_msg


    def msg2json(self):
        """Convert a message object to a JSON object"""

        log.debug('Creating a JSON string from a message object')

        return json.dumps(self.__dict__)

    
    def update(self, package):
        """Update a message object with the parameters supplied by the package (dict)"""

        log.debug('Updating a message object')

        self.__dict__.update(package)


    def __repr__(self):
        """Print the values of a message object"""

        log.debug('Printing a message object')

        # go throguh the variables and collect their names and values
        text = ""
        for key,val in sorted(self.__dict__.items()):
            text += '{} = {}{}'.format(key,val,os.linesep)
        return text.rstrip()


