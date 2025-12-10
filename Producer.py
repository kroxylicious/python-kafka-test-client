#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

from confluent_kafka import Producer
import argparse
import sys

def main(args):
    broker = args.bootstrap_servers
    topic = args.topic
    key = args.key
    headers = args.headers

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    producer_conf = {'bootstrap.servers': broker}

    vargs = vars(args)
    extra_configuration = [x[0].split('=') for x in vargs.get('extra_conf', [])]
    producer_conf.update(dict(extra_configuration))

    # Create Producer instance
    p = Producer(**producer_conf)

    # Optional per-record delivery callback (triggered by poll() or flush())
    # when a record has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Record failed delivery: %s\n' % err)
            sys.exit(1)
        else:
            print('Record {} successfully produced to {} [{}] at offset {}'.format(
                msg.key(), msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka
    for line in sys.stdin:
        try:
            # Produce line (without newline)
            p.produce(topic, headers=headers, key=key, value=line.rstrip(), callback=delivery_callback)

        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d records awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d record.
        p.poll(0)

    # Wait until all records have been delivered
    sys.stdout.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Producer")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", required=True,
                        help="Topic name")
    parser.add_argument('-k', dest="key", default=None,
                        help="Key")
    parser.add_argument('-H', action='append', dest="headers", type=lambda a: tuple(map(str, a.split('='))),
                        default=[], help="Headers (header1=header value)")
    parser.add_argument('-X', nargs=1, dest='extra_conf', action='append', help='Configuration property', default=[])

    main(parser.parse_args())
