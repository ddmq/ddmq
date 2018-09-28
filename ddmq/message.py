import json
import logging as log

class message:
    """Class to represent a single message"""


    def __init__(self, message=None, queue=None, timestamps=None, timeout=None, id=None, priority=None, queue_number=None, filename=None, requeue=None):
        """Initialize a message with the given parameters"""

        log.debug('Initializing message object')

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
            text += '{} = {}\n'.format(key,val)
        return text.rstrip()


