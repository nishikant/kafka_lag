from kafka_lag.common.executor import Execute
import logging
import logging.config


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)s [%(levelname)s] %(message)s',
                        datefmt='%m-%d-%y %H:%M',
                        filename='run.log')

    logger = logging.getLogger(__package__)

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

    parser.add_argument('--consumer_groups', '-c', dest='consumer_groups',
                        nargs='*', required=False,
                        help='Consumergroup for which to get lag. Default all')

    args = parser.parse_args()
    logger.debug("Args are %s ", args)
    ex = Execute(args)
    ex.run()
