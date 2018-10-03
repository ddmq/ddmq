.. ddmq documentation master file, created by
   sphinx-quickstart on Tue Oct  2 13:59:07 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



.. _intro:

.. toctree::
   :maxdepth: 2
   :caption: Introduction:

   introduction

   
.. _submodule:

.. toctree::
   :maxdepth: 2
   :caption: Submodules:

   message
   broker
   



Dead Drop Messaging Queue
=================================

**ddmq** is a file based and serverless messaging queue, aimed at providing a low throughput\* messaging queue when you don't want to rely on a server process to handle your requests. It will create a directory for every queue you create and each message is stored as a JSON objects in a file. *ddmq* will keep track of which messages has been consumed and will requeue messages that have not been acknowledged by the consumers after a set timeout. Since there is no server handling the messages, the houskeeping is done by the clients as they interact with the queue.

*ddmq* is written in Python and should work for both Python 2.7+ and Python 3+, and can also be run as a command-line tool either by specifying the order as options and arguments, or by supplying the operation as a JSON object.

*\* It could handle ~5000-6000 messages per minute on a SSD based laptop (~10% of RabbitMQ on the same hardware), but other processes competing for file access will impact performance.*

Key Features
------------
* serverless
* file based
* first in - first out, within the same priority level
* outputs plain text, json or yaml
* input json packaged operations via command-line
* global and queue specific settings

  - custom message expiry time lengths
  - limit the number of times a message will be requeued after exipry

* message specific settings

  - set custom priority of messages (all integers >= 0 are valid, lower number = higher priority)
  - all other message properties can also be changed per message


Installation
------------
::

    pip install ddmq

Command-Line Usage
------------------

::

    $ ddmq create -f /tmp testmq
    $ ddmq publish /tmp testmq "Hello World!"
    $ ddmq consume /tmp testmq

Python Module Usage
-------------------
::

    import ddmq
    b = broker('/path/to/rootdir', create=True)
    b.publish(queue='queue_name', message='Hello World!')
    msg = b.consume(queue='queue_name')
    print(msg[0].message)





