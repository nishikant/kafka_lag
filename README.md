This package includes modules which will read kafka lag from given zookeeper


	usage: __main__.py [-h] --type TYPE --environment ENVIRONMENT --service
                   SERVICE --region REGION
                   [--consumer_groups [CONSUMER_GROUPS [CONSUMER_GROUPS ...]]]


	Arguments:
    -h, --help            show this help message and exit
	
    --type TYPE, -t TYPE  CMS-Service-Type tag attached to cluster For eg: kafka
	
    --environment ENVIRONMENT, -e ENVIRONMENT
		Environment: for which lag is calculated For eg: qa, int, prod
		
	--service SERVICE, -s SERVICE 
		CMS-Service tag attached to cluster For eg: common
						
	--region REGION, -r REGION
		AWS Region eg: ap-south-1
		
	--consumer_groups, -c [CONSUMER_GROUPS [CONSUMER_GROUPS ...]]                        
		Consumergroup for which to get lag. Default: all
		
	--namespace, -n NAMESPACE
		AWS CloudWatch Namespace. 
		For eg: int_kafka_lag, qa_kafka_lag, perf_kafka_lag, prd_kafka_lag
		 

===

Examples: 

1. To get lag for all consumer groups

	python3 -m kafka_lag -t kafka -s common -e int -r ap-south-1

2. To get lag for specific consumer groups 

	python3 -m kafka_lag -t kafka -s common -e int -r ap-south-1 -c consumer_group1 consumer_group2

