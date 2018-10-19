#! /usr/bin/env python
"""
This file handles the command-line interface.
It will parse the arguments given and call the broker
and message classes as needed.

run 
$ ddmq -h
for more info.
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
import json
import sys
import argparse
import logging as log
import re
import errno

# import extra modules
import yaml
# from ddmq import broker
# from ddmq import message

from broker import broker
from broker import message

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
        if e.error == 'uninitiated':
            sys.exit("The specified root directory ({}) exists but is not initiated. Please run the same command with the (-f) force flag to try to create and initiate directories as needed.".format(root))
        
        elif e.error == 'missing':
            sys.exit("The specified root directory ({}) does not exist. Please run the same command with the (-f) force flag to try to create and initiate directories as needed.".format(root))

    return brokerObj





def view(args=None):
    """
    Handle the command-line sub-command view
    Usage:
    ddmq view [-hfnvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """

    if not args:
        parser = argparse.ArgumentParser(
            description='View available queues and number of messages.',
            usage='''ddmq view [-hfnvd] [--format <plain|json|yaml>] <root> [queue1,queue2,...,queueN]'''
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
        description='Consume message(s) from a queue.',
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
    ddmq ack [-hCrvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Positively acknowledge message(s) from a queue.',
        usage='''ddmq ack [-hCrvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]'''
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
    ddmq nack [-hCrvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Negatively acknowledge message(s) from a queue.',
        usage='''ddmq nack [-hCrvd] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]'''
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






def del_msg(args=None):
    """
    Handle the command-line sub-command del_msg
    Usage:
    ddmq del_msg [-hCvds] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]
    
    Args:
        args:   a pre-made args object, in the case of json being parsed from the command-line

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description='Delete message(s) from a queue.',
        usage='''ddmq del_msg [-hCvds] <root> <queue> <message file1>[,<message file2>,..,<message fileN>]'''
)
    # add available options for this sub-command
    parser.add_argument('root', help="the message queue's root folder")
    parser.add_argument('queue', help="name of the queue the messages are in")
    parser.add_argument('msg_files', help="comma-separated names of file names of the messages to acknowledge")
    parser.add_argument('-C', '--skip-cleaning', action='store_false', help="set to nack the message without doing cleaning of the queue first")
    parser.add_argument('-v', action='store_true', help="verbose mode")
    parser.add_argument('-d', action='store_true', help="debug mode")
    parser.add_argument('-s', action='store_true', help="silent mode")


    # now that we're inside a subcommand, ignore the first two arguments
    args = parser.parse_args(sys.argv[2:])

    # create a broker object
    brokerObj = create_broker(root=args.root, verbose=args.v, debug=args.d)

    # readability
    queue = args.queue
    silent = args.s

    # send the messages to deletion
    deleted_msgs = 0
    for msg_file in args.msg_files.split(','):

        # from IPython.core.debugger import Tracer
        # Tracer()()

        # get the file name
        msg_filename = os.path.basename(msg_file)

        # is the filename of a consumed message?
        match = re.search('^\d+\.\d+\.\d+\.ddmq[a-zA-Z0-9]+$', msg_filename)
        if match:
            msg_filename = os.path.join(args.root, queue, 'work', msg_filename)
        
        # is the filename of a not yet consumed message?
        match = re.search('^\d+\.\d+\.ddmq[a-zA-Z0-9]+$', msg_filename)
        if match:
            msg_filename = os.path.join(args.root, queue, msg_filename)

        # make sure the file exists
        if not os.path.isfile(msg_filename):
            if not silent:
                print("Skipping {}, file does not exists".format(msg_filename))
            continue

        if brokerObj.delete_message(msg_filename):
            if not silent:
                print("Deleted {}".format(msg_filename))
            deleted_msgs += 1


    if not silent and deleted_msgs>1:
        print('Deleted {} messages'.format(deleted_msgs))





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
    parser.add_argument('json_payload', help="the json object, within single quotes", type=str)
    args = parser.parse_args(sys.argv[2:])

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

    try:
        # read the payload
        payload = json.loads(args.json_payload)
    except ValueError:
        sys.exit("Error: unable to load the JSON object (wrong format or missing single quotes?)\n\nExample of structure and all the options that can be set:\n\n'{}'".format(json.dumps(options)))

    

    # apply the payload over the defaults
    options.update(payload)

    # transfer the options to the args object
    for key,val in options.items():
        vars(args)[key] = val

    # use dispatch pattern to invoke method with same name
    eval(args.command)(args=args)




# TODO
# def search():
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
del_msg   Delete the specified message
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
del_msg   Delete the specified message
purge     Purge all messages from queue
clean     Clean out expired messages from queue
json      Run a command packaged as a JSON object

For more info about the commands, run
ddmq <command> -h 

Documentation and code:
https://ddmq.readthedocs.io
https://github.com/ddmq/ddmq
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
    elif args.command not in ['view', 'create', 'delete', 'publish', 'consume', 'ack', 'nack', 'del_msg', 'purge', 'clean', 'json']:
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