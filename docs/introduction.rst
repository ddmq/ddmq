.. ddmq documentation master file, created by
   sphinx-quickstart on Tue Oct  2 13:59:07 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Dead Drop Messaging Queue
=========================

**ddmq** is a file based and serverless messaging queue, aimed at providing a low throughput\* messaging queue when you don't want to rely on a server process to handle your requests. It will create a directory for every queue you create and each message is stored as a JSON objects in a file. *ddmq* will keep track of which messages has been consumed and will requeue messages that have not been acknowledged by the consumers after a set timeout. Since there is no server handling the messages, the houskeeping is done by the clients as they interact with the queue.

*ddmq* is written in Python and should work for both Python 2.7+ and Python 3+, and can also be run as a command-line tool either by specifying the order as options and arguments, or by supplying the operation as a JSON object.

Requirements
------------
| Python 2.6+ or 3+, should work with both.
| Additional modules **required**: pyyaml
| Additional modules *recommended*: beautifultable

Installation
------------
::

    $ pip install ddmq


Key Features
------------
* serverless
* file based
* First in - first out, within the same priority level
* outputs plain text, json or yaml
* input json packaged operations via command-line
* global and queue specific settings

  - custom message expiry time lengths
  - limit the number of times a message will be requeued after exipry

* message specific settings

  - set custom priority of messages (all integers >= 0 are valid, lower number = higher priority)
  - all other message properties can also be changed per message


Command-Line Usage
------------------

::

    usage: ddmq <command> [<args>]

    The available commands are:
    view      List queues and number of messages
    create    Create a queue
    delete    Delete a queue
    publish   Publish message to queue
    consume   Consume message from queue
    purge     Purge all messages from queue
    clean     Clean out expired messages from queue
    json      Run a command packaged as a JSON object

    For more info about the commands, run
    ddmq <command> -h 

    Command-line interface to Dead Drop Messaging Queue (ddmq).

    positional arguments:
    command        Subcommand to run

    optional arguments:
    -h, --help     show this help message and exit
    -v, --version  print version

    
    Examples: 
    
    # create a new queue and publish a message to it
    $ ddmq create -f /tmp testmq
    $ ddmq publish /tmp testmq "Hello World!"

    # consume a message from a queue
    $ ddmq consume /tmp testmq

    # view all queues present in the specified root directory
    $ ddmq view /tmp

    # remove all messages from a queue
    $ ddmq purge /tmp testmq

    # delete a queue
    $ ddmq delete /tmp testmq


Python Module Usage
-------------------
::

    # imports both the broker and message module
    import ddmq

    # create the broker object and specify the path to the root directory
    # adding create=True to tell it to create and initiate both the root 
    # directory and queue directories if they don't already exist
    b = broker('/path/to/rootdir', create=True)

    # publish a message to the specified queue
    b.publish(queue='queue_name', message='Hello World!')

    # consume a single message from the specified queue
    msg = b.consume(queue='queue_name')

    # print the message contained
    print(msg[0].message)




File Structure
--------------
The structure ddmq uses to handle the messages consists of a root directory, with subfolders for each created queue. The messages waiting in a queue are stored in the queue's folder, and messages that have been consumed but not yet acknowledged are stored in the queue's work directory.

::

    root/
    ├── ddmq.yaml
    ├── queue_one
    │   ├── 999.3.ddmqfc24476c6708416caa2a101845dddd9a
    │   ├── ddmq.yaml
    │   └── work
    │       ├── 1538638378.999.1.ddmq39eb64e1913143aa8d28d9158f089006
    │       └── 1538638379.999.2.ddmq1ed12af3760e4adfb62a9109f9b61214
    └── queue_two
        ├── 999.1.ddmq6d8742dbde404d5ab556bf229151f66b
        ├── 999.2.ddmq15463a6680f942489d54f1ec78a53673
        ├── ddmq.yaml
        └── work

In the example above there are two queues created (queue_one, queue_two) and both have messages published to them. In queue_one there are two messages that have been consumed already, but not yet acknowledged (*acked*), so the messages are stored in the queue_one's work folder. As soon as a message is acked the message will be deleted by default. Messages that are negatively acknowledged (*nacked*) will be requeue by default.

Both the root directory and each queue subfolder will contain config files named *ddmq.yaml* that contains the settings to be used. The root's config file will override the default values, and the queue's config files will override both the default values and the root's config file. If a message is given specific settings when being published/consumed, these settings will override all the ddmq.yaml files.



ddmq.yaml
---------


Use case
--------
Since ddmq handles one file per message it will be much slower than other queues. A quick comparison with RabbitMQ showed that first publishing and then consuming 5000 messages is about 10x slower using ddmq (45s vs 4.5s). The point of ddmq is not performance, but to be used in environments where you can't run a server for some reason.

My own motivation for writing ddmq was to run on a shard HPC cluster where I could not reliably run a server process on the same node all the time. The mounted network storage system was available everywhere and all the time though. The throughput was expected to be really low, maybe <10 messages per day so performance was not the main focus.


Index
=====

* :ref:`genindex`

