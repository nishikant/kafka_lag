import boto3
import logging

logger = logging.getLogger(__name__)


class AWSQuery:
    def __init__(self, type, environment, service, region):
        self.type = type
        self.environment = environment
        self.service = service
        self.region = region

    def get_zookeeper_ips(self):
        logger.debug("type: %s", self.type)
        logger.debug("environment: %s", self.environment)
        logger.debug("service: %s", self.service)

        ec2 = boto3.client('ec2', region_name=self.region)

        filter = [
            {
                'Name': 'tag:cms-service-type',
                'Values': [self.type]
            }, {
                'Name': 'tag:cms-environment',
                'Values': [self.environment]
            }, {
                'Name': 'tag:cms-service',
                'Values': [self.service]
            }
        ]

        logger.debug("Filters used: %s", filter)

        response = ec2.describe_instances(Filters=filter)
        logger.debug("call respons %s", response)

        service_ips = []

        for row in response['Reservations']:
            for instance in row['Instances']:
                service_ips.append(instance['PrivateIpAddress'])
                logger.debug("Ipaddr: %s", instance['PrivateIpAddress'])

        return(service_ips)

    def publish_metrics_cloudwatch(self, consumer_group, topic, lag_value):

        cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        logger.debug("Pushing data %s %s %s to aws", consumer_group,
                     topic, lag_value)

        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': self.service,
                    'Dimensions': [
                        {
                            'Name': 'consumer_group_name',
                            'Value': consumer_group
                        },
                    ],
                    'Value': lag_value
                },
            ],
            Namespace='kafka_lag'
        )
