# Dead Drop Messaging Queue
A file based serverless messaging queue

## Requirements
Python 2.6+ or 3+, should work with both. 
Additional modules required: pyyaml
Additional modules recommended: beautifultable

## Usage
Run as a commandline tool or import in a python script.

```
usage: ddmq.py <command> [<args>]

The available commands are:
view      List queues and number of messages
create    Create a queue
delete    Delete a queue
publish   Publish message to queue
consume   Consume message from queue
purge     Purge all messages from queue
```
