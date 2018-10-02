#!/usr/bin/env python
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import json
import boto3
from boto3.session import Session
from argparse import ArgumentParser
import logging
import datetime
from dateutil.tz import *

container_instance_ec2_mapping = {}

def putTasks(region, cluster, task):
    id_name = 'taskArn'
    task_id = task["taskArn"]
    new_record = {}

    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table("ECSTaskStatus")
    saved_task = table.get_item( Key = { id_name : task_id } )
        
    # Look first to see if you have received this taskArn before.
    # If not,
    #   - you are getting a new task - i.e. the script is being run for the first time.
    #   - store its details in DDB
    # If yes,
    #   - the script is being run after the solution has been deployed.
    #   - dont do anything. quit.
    if "Item" in saved_task:
            print("Task: %s already in the DynamoDB table." % (task_id) )
            return 1
    else:
        new_record["launchType"]    = task["launchType"]
        new_record["region"]        = region
        new_record["clusterArn"]    = task["clusterArn"]
        new_record["cpu"]           = task["cpu"]
        new_record["memory"]        = task["memory"]
        if new_record["launchType"] == 'FARGATE':
            new_record["containerInstanceArn"]  = 'INSTANCE_ID_UNKNOWN'
            (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = ('INSTANCE_TYPE_UNKNOWN', 'linux', 'INSTANCE_ID_UNKNOWN')
        else:
            new_record["containerInstanceArn"]  = task["containerInstanceArn"]
            (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = getInstanceType(region, task['clusterArn'], task['containerInstanceArn'], task['launchType'])
            
        if ':' in task["group"]:
            new_record["group"], new_record["groupName"] = task["group"].split(':')
        else:
            new_record["group"], new_record["groupName"] = 'taskgroup', task["group"]

        # Convert startedAt time to UTC from local timezone. The time returned from ecs_describe_tasks() will be in local TZ.
        startedAt = task["startedAt"].astimezone(tzutc())
        new_record["startedAt"]     = datetime.datetime.strftime(startedAt, '%Y-%m-%dT%H:%M:%S.%fZ')
        new_record["taskArn"]       = task_id
        new_record['stoppedAt'] = 'STILL-RUNNING'
        new_record['runTime'] = 0

        table.put_item( Item=new_record )
        return 0
            
def getInstanceType(region, cluster, instance, launchType):
    instanceType    = 'INSTANCE_TYPE_UNKNOWN'
    osType          = 'linux'
    instanceId      = 'INSTANCE_ID_UNKNOWN'

    global container_instance_ec2_mapping
    
    # Shouldnt care about isntanceType if this is a FARGATE task
    if launchType == 'FARGATE':
        return (instanceType, osType, instanceId)
   
    if instance in container_instance_ec2_mapping:
        (instanceId, instanceType) = container_instance_ec2_mapping[instance]
        return (instanceType, osType, instanceId)

    ecs = boto3.client("ecs", region_name=region)
    try:
        result = ecs.describe_container_instances(cluster=cluster, containerInstances=[instance])
        if result and 'containerInstances' in result:
            attr_dict = result['containerInstances'][0]['attributes']
            
            instanceId = result['containerInstances'][0]["ec2InstanceId"]
            
            instance_type = [d['value'] for d in attr_dict if d['name'] == 'ecs.instance-type']
            if len(instance_type):
                # Return the instanceType. In addition, store this value in a DynamoDB table.
                instanceType = instance_type[0]
            
            os_type = [d['value'] for d in attr_dict if d['name'] == 'ecs.os-type']
            if len(os_type):
                # Return the osType. In addition, store this value in a DynamoDB table.
                osType = os_type[0]
        container_instance_ec2_mapping[instance] = (instanceId, instanceType)    
        return (instanceType, osType, instanceId)
    except:
        # Try finding the instanceType in DynamoDB table
        return (instanceType, osType, instanceId)
        
if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('--region',  '-r', required=True, help="AWS Region in which Amazon ECS service is running.")
    parser.add_argument("-v", "--verbose", action="store_true")

    cli_args = parser.parse_args()
    region = cli_args.region

    if cli_args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    ecs = boto3.client("ecs", region_name=region)
    response = ecs.list_clusters()

    clusters = []
    if 'clusterArns' in response and response['clusterArns']:
        clusters = response['clusterArns']

    tasks = []
    for cluster in clusters:
        nextToken = ''
        while True:
            response = ecs.list_tasks(cluster=cluster, maxResults=100, nextToken = nextToken)
            tasks = tasks + [(cluster, taskArn) for taskArn in response['taskArns'] ]
            if 'nextToken' in response and response['nextToken']:
                nextToken = response['nextToken']
            else:
                break

    for (cluster, task) in tasks:
        # Use range function to get maybe 10 tasks at a time.
        #taskDetails = ecs.describe_tasks(cluster=cluster, tasks=[task])

        taskDetails = ecs.describe_tasks(cluster=cluster, tasks=[task])

        # Get all tasks in the cluster and make an entry in DDB.
        tasks = putTasks(region, cluster, taskDetails['tasks'][0])
