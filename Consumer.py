#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
from typing import AnyStr

from confluent_kafka import Consumer, KafkaException
import argparse
import sys
import logging
import json
import inspect

def get_value_from_type(obj):
    value_str = obj
    if isinstance(obj, bytes):
        try:
            value_str = obj.decode("utf-8")
        except UnicodeDecodeError:
            value_str = "".join(map(chr, obj))
    if isinstance(obj, list):
        value_str = [get_value_from_type(x) for x in obj]
    if isinstance(obj, tuple):
        value_str = { "Key" : str(get_value_from_type(obj[0])),
                      "Value": str(get_value_from_type(obj[1])) }

    return value_str

def props(obj):
    pr = {}
    for name in dir(obj):
        value = getattr(obj, name)
        if (not (name.startswith('__') or name.startswith("set_"))
                and not inspect.ismethod(value)):
            pr[name] = get_value_from_type(value())
    return pr

def print_record_json(msg):
    res = json.dumps(props(msg))
    print("Received: " + res)

def main(args):
    topic = args.topic
    records_expected = int(args.num_of_records)

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    consumer_conf = {'bootstrap.servers': args.bootstrap_servers, 'group.id': args.group, 'session.timeout.ms': 6000,
                     'auto.offset.reset': 'earliest', 'enable.auto.offset.store': False}

    vargs = vars(args)
    extra_configuration = [x[0].split('=') for x in vargs.get('extra_conf', [])]
    consumer_conf.update(dict(extra_configuration))

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log records
    c = Consumer(consumer_conf, logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe([topic], on_assign=print_assignment)

    # Read records from Kafka, print to stdout
    try:
        records_received = 0
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                print_record_json(msg)
                # Store the offset associated with msg to a local cache.
                # Stored offsets are committed to Kafka by a background thread every 'auto.commit.interval.ms'.
                # Explicitly storing offsets after processing gives at-least once semantics.
                c.store_offsets(msg)
                records_received += 1
            if records_received == records_expected:
                c.close()
                sys.exit(0)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-n', dest="num_of_records", default=0,
                        help="Number of records expected")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="test_group",
                        help="Consumer group")
    parser.add_argument('-X', nargs=1, dest='extra_conf', action='append', help='Configuration property', default=[])

    main(parser.parse_args())
