
import logging
from confluent_kafka.admin import ConfigResource
from confluent_kafka import KafkaException
import confluent_kafka
import concurrent.futures

logger = logging.getLogger(__name__)


class ConfluenceKafkaQuery:

    def __init__(self, kafka_client):
        self.kafka_client = kafka_client

    def listTopics(self):
        try:
            logger.debug("Listing Topics")
            topic_list = self.kafka_client.list_topics(timeout=-1)
            logger.debug("Topics are: %s", topic_list)
            return(topic_list)
        except KafkaException as e:
            logger.debug("Some issue listing all topics: %s", e)

    def getTopicDescription(self, topic):

        topic_configResource = self.kafka_client.describe_configs([
            ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic)
        ])

        for j in concurrent.futures.as_completed(iter(
                topic_configResource.values())):
            config_response = j.result(timeout=1)
            logger.debug("config_response: %s", config_response)


class KPKafkaQuery:

    def __init__(self, kafka_client):
        self.kafka_client = kafka_client

    def listConsumerGroups(self):
        list_groups_request = self.kafka_client.list_consumer_groups()
        logger.debug("Consumer Groups are : %s", list_groups_request)

        return(list_groups_request)

    def describeConsumerGroups(self, consumer_group):

        group_detail = self.kafka_client.\
            describe_consumer_groups(consumer_group)
        return(group_detail)

    def getTopicEndOffset(self, topic_partition):
        logger.debug('TP for end_offset is %s ', topic_partition)
        end_offset = self.kafka_client.end_offsets(topic_partition)

        return(end_offset)
