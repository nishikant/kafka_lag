from . import kafka
from . import kafka_query

from kafka_lag.aws_query import AWSQuery
import logging
import random

import json

logger = logging.getLogger(__name__)


class Execute:

    def __init__(self, args):
        self.args = args
        self.aws = AWSQuery(args.type, args.environment,
                            args.service, args.region)

    def run(self):
        server_ips = self.aws.get_zookeeper_ips()
        ip = random.choice(server_ips)
        self.queryKPKafka(ip)

    def getTopicEndOffset(self, topic, ip):
        consumer = kafka.KPKafka(ip).create_consumer('lag_collector', topic)
        end_offset = consumer.end_offsets()
        return(end_offset)

    def _query_loop(self, consumer_group):
        pass

    def _push_to_cloudwatch(self, kafka_lag):
        pass

    def queryKPKafka(self, ip):

        logger.debug("Creating admin client")
        admin_kafka_client = kafka.KPKafka(ip).create_admin_client()
        admin_query = kafka_query.KPKafkaQuery(admin_kafka_client)
        if self.args.consumer_groups:
            consumer_groups = self.args.consumer_groups
        else:
            consumer_groups = admin_query.listConsumerGroups()

        logger.debug("Creating kafka consumer")
        kafka_consumer = kafka.KPKafka(ip).create_consumer('lag_collector')
        consumer_query = kafka_query.KPKafkaQuery(kafka_consumer)

        logger.debug("list of consumer groups is: %s", consumer_groups)

        cg_lag = {}
        end_offset = {}

        for consumer_group in consumer_groups:
            logger.debug("Working on CG %s ", consumer_group)
            if not isinstance(consumer_group, tuple) or \
               consumer_group[1] == 'consumer':

                cg_name = consumer_group[0] if \
                    isinstance(consumer_group, tuple) else consumer_group

                cg_lag[cg_name] = {}

                logger.debug("Adding group %s", cg_name)

                current_cg_offset = admin_kafka_client.\
                    list_consumer_group_offsets(cg_name)

                logger.debug("Current CG Offset is %s ", current_cg_offset)

                partition_list = []
                cg_offsets = {}
                cg_offsets[cg_name] = {}

                for tp in current_cg_offset:
                    partition_list.append(tp)
                    logger.debug('partition list is %s ', partition_list)
                    cg_offsets[tp] = current_cg_offset[tp].offset

                end_off = consumer_query.getTopicEndOffset(partition_list)
                for eo in end_off:
                    end_offset[eo] = end_off[eo]
                    logger.debug("EO is %s TP is %s", end_off[eo], eo)
                    try:
                        cg_lag[cg_name][eo.topic]
                    except KeyError:
                        cg_lag[cg_name][eo.topic] = 0

                    logger.debug("cg_lag[cg_name][eo.topic] = %s",
                                 cg_lag[cg_name][eo.topic])
                    logger.debug("end_offset[eo] = %s", end_offset[eo])
                    logger.debug("cg_offsets[eo] = %s", cg_offsets[eo])
                    cg_lag[cg_name][eo.topic] += end_offset[eo] - \
                        cg_offsets[eo]

        for cg in cg_lag:
            cg_total_lag = 0

            for topic in cg_lag[cg]:
                logger.debug("For CG %s topic %s lag is %s", cg, topic,
                             cg_lag[cg][topic])
                cg_total_lag += cg_lag[cg][topic]

                if cg_total_lag > 0:
                    self.aws.publish_metrics_cloudwatch(cg, topic,
                                                        cg_lag[cg][topic])

        logger.info("CG Lag is %s ", json.dumps(cg_lag))
        return(json.dumps(cg_lag))

    def queryConfluenceKafka(self, ip):

        try:
            kafka_client = kafka.ConfluenceKafka(ip).create_admin_client()
        except ConnectionError as e:
            logger.debug("Issue connecting ConfluenceKafka IP %s with MSG %s",
                         ip, e)

        query = kafka_query.ConfluenceKafkaQuery(kafka_client)
        topic_future = query.listTopics()
        logger.debug("List of Topics: %s", topic_future.topics.values())

        topics = []
        for topic in topic_future.topics.values():
            if topic.error is None:
                topics.append(topic)
                logger.debug("Topic name: %s", topic)

                query.getTopicDescription(str(topic))
