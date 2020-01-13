from confluent_kafka.admin import AdminClient
from kafka.admin.client import KafkaAdminClient
from kafka import KafkaConsumer, TopicPartition
from kafka import BrokerConnection
from kafka.protocol.admin import *
import time
import logging

logger = logging.getLogger(__name__)


class ConfluenceKafka:

    def __init__(self, server_ip):
        self.server_ip = server_ip
        logger.debug("Creating ConfluenceKafka Client")

    def create_admin_client(self):
        # Creation of config
        logger.debug("CKafka admin_client : %s", self.server_ip)
        conf = {'bootstrap.servers': 'localhost',
                'session.timeout.ms': 6000}

        admin_client = AdminClient(conf)
        logger.debug("Creating CKafka AdminClient: %s", admin_client)
        # admin_client.list_topics(timeout=10)
        return(admin_client)


class KPKafka:
    def __init__(self, server_ip):
        self.server_ip = server_ip
        logger.debug("Creating kafka-python client")

    def create_admin_client(self):
        logger.debug("Creating BrokerConnection to %s", self.server_ip)
        try:
            admin_client = KafkaAdminClient(bootstrap_servers='172.28.48.52',
                                            request_timeout_ms=60000)
            wait_count = 15
            while(admin_client is None and wait_count > 0):
                logger.debug("Waiting for admin_client")
                wait_count -= 1
                time.sleep(1)
            return(admin_client)
        except ConnectionError as e:
            logger.debug("Some issue connecting to %s with message %s",
                         self.server_ip, e)

    def create_consumer(self, consumer_group):

        logger.debug("Creating Normal Consumer")
        try:
            kafka_consumer = KafkaConsumer(
                bootstrap_servers=self.server_ip,
                group_id=consumer_group,
                enable_auto_commit=False
            )
            return(kafka_consumer)
        except Exception as e:
            logger.debug("Error creating consumer for consumer_group %s",
                         consumer_group)
            logger.debug(e)
