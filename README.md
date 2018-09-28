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

## Use case
Since ddmq handles one file per message it will be much slower than other queues. A quick comparison with RabbitMQ showed that first publishing and then consuming 5000 messages is about 10x slower using ddmq (45s vs 4.5s). The point of ddmq is not performance, but to be used in environments where you can't run a server for some reason.

My own motivation for writing ddmq was to run on a shard HPC cluster where I could not reliably run a server process on the same node all the time. The mounted network storage system was available everywhere and all the time though. The throughput was expected to be really low, maybe <10 messages per day so performance was not the main focus.