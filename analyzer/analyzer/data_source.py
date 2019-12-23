from confluent_kafka import Consumer, KafkaError
import threading
import json
import logging
import time

from . import config
from .log import logger

from .parse_data import parse_data
from .process_data import process_data


def stats_cb(stats_json_str):
    logger.info(f'KAFKA Stats: {json.loads(stats_json_str)}')


def start():
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    ip, port = config.get_kafka_address()
    conf = {
        'bootstrap.servers': f"{ip}:{port}",
        'group.id': "GJDW",
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'stats_cb': stats_cb,
    }

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logging.getLogger('consumer'))

    rcv_cnt = 0

    def print_assignment(consumer, partitions):
        logger.info(f'KAFKA consumer start! Assignment: {partitions} running...')

    # Subscribe to topics
    c.subscribe(['GJDW_HTTP_MSG'], on_assign=print_assignment)
    count = 0
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    # logger.info('topic {} reached end at offset {}, partition {}, rcv cnt {}'.format(
                    #     msg.topic(), msg.offset(), msg.partition(), rcv_cnt))
                    # gjdw_process.save_all()
                    pass
                else:
                    logger.error('topic {} receive error code {}, partition {}, offset {}, rcv cnt {}'.format(
                        msg.topic(), msg.error().code(), msg.partition(), msg.offset(), rcv_cnt))
            else:
                count += 1
                data = parse_data(msg)
                if data:
                    process_data(data)
                if count%10000 == 0:
                    logger.info(f"process {count}")

    except Exception:
        c.close()  # we don't close consumer
        logger.critical("Got exception:", exc_info=True)


class KafkaConsumerThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        while True:
            try:
                start()
            except Exception:
                # c.close()  # we don't close consumer
                logger.critical("Got exception:", exc_info=True)
                time.sleep(60)


def data_source_init():
    KafkaConsumerThread('KafkaConsumerThread').start()
