#!/usr/bin/python
# Boto 3
import boto3
import sys
ec2 = boto3.resource('ec2')

instances = ec2.create_instances(
        ImageId='ami-5fe20e30', 
        InstanceType='t2.micro', 
        SecurityGroups=['Hadoop'],
        SecurityGroupIds=['sg-fc744f95'],
        MinCount=int(sys.argv[1]), 
        MaxCount=int(sys.argv[1]))

slaves = open('/usr/local/hadoop/etc/hadoop/slaves', 'w')
for instance in instances:
    slaves.write(str(instance.private_dns_name) + '\n')
    print instance.public_dns_name
    print instance.private_dns_name
    print

slaves.close()
