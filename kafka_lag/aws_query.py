import boto3
import logging

logger = logging.getLogger(__name__)


class AWSQuery:
    def __init__(self, type, environment, service, region):
        self.type = type
        self.environment = environment
        self.service = service
        self.region = region

    def get_ips(self):
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
