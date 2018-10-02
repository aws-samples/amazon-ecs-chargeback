import json
import boto3
from boto3.session import Session
import datetime

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


def lambda_handler(event, context):
    id_name = "taskArn"

    new_record = {}
    # For debugging so you can see raw event format.
    print('Here is the event:')
    print(json.dumps(event))

    if event["source"] != "aws.ecs" and event["detail-type"] != "ECS Task State Change":
        raise ValueError("Function only supports input from events with a source type of: aws.ecs and of type - ECS Task State Change -")

    if event["detail"]["lastStatus"] == event["detail"]["desiredStatus"]:
        event_id = event["detail"]["taskArn"]

        s = Session()
        cur_region = s.region_name
        dynamodb = boto3.resource("dynamodb", region_name=cur_region)
        table = dynamodb.Table("ECSTaskStatus")
        saved_event = table.get_item( Key = { id_name : event_id } )
        
        # Look first to see if you have received this taskArn before.
        # If not,
        #   - you are getting a new task that has just started, or the Lambda solution was deployed
        #     after the task started and it is being stopped now.
        #   - store its details in DDB
        # If yes,
        #   - that just means that you are receiving a task change - mostly a stop event.
        #   - store the stop time in the task item in DDB
        if "Item" in saved_event:
            if event["detail"]["lastStatus"] == "STOPPED":
                #table.update_item( Key= { id_name : event_id },
                #   AttributeUpdates= {
                #       'stoppedAt': {'S':  event["detail"]["stoppedAt"]},
                #   },
                #)
                table.update_item( Key= { id_name : event_id },
                    UpdateExpression="set stoppedAt = :d, runTime=:t",
                    ExpressionAttributeValues={
                        ':d': str(event["detail"]["stoppedAt"]),
                        ':t': getRunTime(event["detail"]["startedAt"], event["detail"]["stoppedAt"])
                    },
                    ReturnValues="UPDATED_NEW"
                )
                print("Saving updated event - ID " + event_id)
        else:
            # This could be if the task has just started, or
            # The Lambda is deployed after the task has started running.
            #   In this case, the task event will only be raised when it is stopped.
            new_record["launchType"]    = event["detail"]["launchType"]
            new_record["region"]        = event["region"]
            new_record["clusterArn"]    = event["detail"]["clusterArn"]
            new_record["cpu"]           = event["detail"]["cpu"]
            new_record["memory"]        = event["detail"]["memory"]
            if new_record["launchType"] == 'FARGATE':
                new_record["containerInstanceArn"]  = 'INSTANCE_ID_UNKNOWN'
                (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = ('INSTANCE_TYPE_UNKNOWN', 'linux', 'INSTANCE_ID_UNKNOWN')
            else:
                new_record["containerInstanceArn"]  = event["detail"]["containerInstanceArn"]
                (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = getInstanceType(event['region'], event['detail']['clusterArn'], event['detail']['containerInstanceArn'], event['detail']['launchType'])

            if ':' in event["detail"]["group"]:
                new_record["group"], new_record["groupName"] = event["detail"]["group"].split(':')
            else:
                new_record["group"], new_record["groupName"] = 'taskgroup', event["detail"]["group"]

            # Not provided in FARGATE - new_record["pullStartedAt"] = event["detail"]["pullStartedAt"]
            new_record["startedAt"]     = event["detail"]["startedAt"]
            new_record["taskArn"]       = event_id
            new_record['stoppedAt'] = 'STILL-RUNNING'
            new_record['runTime'] = 0

            if event["detail"]["lastStatus"] == "STOPPED":
                new_record['stoppedAt']     = event["detail"]["stoppedAt"]
                new_record['runTime']       = getRunTime(event["detail"]["startedAt"], event["detail"]["stoppedAt"])
                        
            table.put_item( Item=new_record )
            print("Saving new event - ID " + event_id)
            
def getInstanceType(region, cluster, instance, launchType):
    instanceType    = 'INSTANCE_TYPE_UNKNOWN'
    osType          = 'linux'
    instanceId      = 'INSTANCE_ID_UNKNOWN'
    
    # Shouldnt care about isntanceType if this is a FARGATE task
    if launchType == 'FARGATE':
        return (instanceType, osType, instanceId)
    
    ecs = boto3.client("ecs")
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
            
        # Else - if describe_instances doesnt return a result, make a last attempt check in DynamoDB table
        # that keeps a mapping of containerInstanceARN to instanceType
        return (instanceType, osType, instanceId)
    except:
        # Try finding the instanceType in DynamoDB table
        return (instanceType, osType, instanceId)
        
def getRunTime(startTime, stopTime):
    runTime = '0.0'
    start = datetime.datetime.strptime(startTime, '%Y-%m-%dT%H:%M:%S.%fZ')
    stop = datetime.datetime.strptime(stopTime, '%Y-%m-%dT%H:%M:%S.%fZ')
    runTime = (stop-start).total_seconds()
    return int(round((runTime)))

