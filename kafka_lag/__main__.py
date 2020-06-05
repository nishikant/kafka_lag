from kafka_lag.common.executor import Execute
import logging
import logging.config
import json
from collections import namedtuple

# logging.basicConfig(level=logging.DEBUG,
#                    format='%(asctime)s %(name)s [%(levelname)s] %(message)s',
#                    datefmt='%m-%d-%y %H:%M')

logger = logging.getLogger(__package__)
logger.setLevel(logging.INFO)

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Process Kafka Cluster lag')
    parser.add_argument('--type', '-t', dest='type', required=True,
                        help='CMS-Service-Type tag attached to cluster\
                        For eg: kafka')

    parser.add_argument('--environment', '-e', dest='environment',
                        required=True,
                        help='Environment for which lag is calculated\
                        For eg: qa, int, prod')

    parser.add_argument('--service', '-s', dest='service', required=True,
                        help='CMS-Service tag attached to cluster\
                        For eg: common')

    parser.add_argument('--region', '-r', dest='region', required=True,
                        help='AWS Region eg: ap-south-1')

    parser.add_argument('--namespace', '-n', dest='namespace', required=True,
                        help='AWS CloudWatch namespace eg: int_kafka_lag, \
                        qa_kafka_lag, perf_kafka_lag, prd_kafka_lag')

    parser.add_argument('--consumer_groups', '-c', dest='consumer_groups',
                        nargs='*', required=False,
                        help='Consumergroup for which to get lag. Default all')

    args = parser.parse_args()
    logger.debug("Args type is %s ", type(args))
    logger.debug("Args are %s ", args)
    ex = Execute(args)
    ex.run()


def _json_object_hook(data):
    return namedtuple('args', data.keys())(*data.values())


def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)


def lambda_handler(event, context):
    logger.debug("Executing lambda_handler code")
    logger.debug("Type is : %s", event['type'])
    logger.debug("Environment is : %s", event['environment'])
    logger.debug("Service is : %s", event['service'])
    logger.debug("Region is : %s", event['region'])
    logger.debug("CloudWatch Namespace is : %s", event['namespace'])
    try:
        event['consumer_groups']
        logger.debug("Getting lag for Consumer Groups %s",
                     event['consumer_groups'])
    except KeyError:
        logger.debug("Consumer Group list not given",
                     "So going to get lag for all consumer groups")
    finally:
        args = json2obj(json.dumps(event))
        logger.debug("Args are %s ", args)
        print("Args are %s", args)

        ex = Execute(args)
        lag_json = ex.run()
        return(lag_json)
