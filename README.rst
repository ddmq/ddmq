Dead Drop Messaging Queue
=========================

A file based serverless messaging queue

Requirements
------------
| Python 2.6+ or 3+, should work with both.
| Additional modules **required**: pyyaml
| Additional modules *recommended*: beautifultable

Installation
------------
::

    pip install ddmq

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
    json      Run a command packaged as a JSON object

    For more info about the commands, run
    ddmq <command> -h 

    Command-line interface to Dead Drop Messaging Queue (ddmq).

    positional arguments:
    command        Subcommand to run

    optional arguments:
    -h, --help     show this help message and exit
    -v, --version  print version


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

Use case
--------
Since ddmq handles one file per message it will be much slower than other queues. A quick comparison with RabbitMQ showed that first publishing and then consuming 5000 messages is about 10x slower using ddmq (45s vs 4.5s). The point of ddmq is not performance, but to be used in environments where you can't run a server for some reason.

My own motivation for writing ddmq was to run on a shard HPC cluster where I could not reliably run a server process on the same node all the time. The mounted network storage system was available everywhere and all the time though. The throughput was expected to be really low, maybe <10 messages per day so performance was not the main focus.
