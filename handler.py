import json
import snowflake.connector
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import urllib.parse
import boto3
import time
from requests_aws4auth import AWS4Auth
import requests
import os
from keypair_auth import get_snowflake_cursor

def _get_response(status_code, body):
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status_code, "body": body}

def _get_body(event):
    try:
        return json.loads(event.get("body", ""))
    except:
        logger.debug("event body could not be JSON decoded.")
        return {}

def connection_manager(event, context):
    """
    Handles connecting and disconnecting for the Websocket.
    """
    connectionID = event["requestContext"].get("connectionId")

    if event["requestContext"]["eventType"] == "CONNECT":
        print("Connect requested: ", connectionID)
        return _get_response(200, "Connect successful.")

    elif event["requestContext"]["eventType"] == "DISCONNECT":
        logger.info("Disconnect requested")
        return _get_response(200, "Disconnect successful.")

    else:
        logger.error("Connection manager received unrecognized eventType '{}'")
        return _get_response(500, "Unrecognized eventType.")

def _get_postback_url(event):
    requestContext = event['requestContext']
    domain = requestContext['domainName']
    stage = requestContext['stage']
    connectionId = requestContext['connectionId']

    postbackURL = 'https://' + domain + '/' + stage + '/%40connections/' + urllib.parse.quote_plus(connectionId)

    return postbackURL

def default_message(event, context):
    """
    Send back error when unrecognized WebSocket action is received.
    """
    logger.info("Unrecognized WebSocket action received.")
    return _get_response(400, "Unrecognized WebSocket action.")


def ping(event, context):
    """
    Sanity check endpoint that echoes back 'PONG' to the sender.
    """
    logger.info("Ping requested.")
    return _get_response(200, "PONG!")

def run_view(event, context):
    #print("event: ", json.dumps(event))
    postbackURL = _get_postback_url(event)
    print("postbackURL: ", postbackURL)
    message = postbackURL

    if 'body' in event:
        body = json.loads(event['body'])

        if 'action' in body:
            action = body['action']
            if action == 'run_view':
                view_name = body['view_name']

                input = {}
                input['view_name'] = view_name
                input['post_back_url'] = postbackURL
                input['wait_time'] = 5
                aws_account = boto3.client('sts').get_caller_identity()['Account']

                client = boto3.client('stepfunctions')
                response = client.start_execution(
                    stateMachineArn=os.environ['SNOWFLAKE_STATE_MACHINE_ARN'],
                    name='execution-' + time.strftime("%Y%m%d%H%M%S"),
                    input=json.dumps(input)
                )
                message = "Request submitted. Please wait..."
                auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], 'us-east-1', 'execute-api', session_token = os.environ['AWS_SESSION_TOKEN'])
                r = requests.post(postbackURL, auth=auth, data=message)
                print(r.status_code)


    return _get_response(200, message)


def fetch_results(event, context):
    if 'body' in event:
        body = json.loads(event['body'])


        if 'query_id' in body:
            query_id = body['query_id']
            if 'offset' in body:
                offset = body['offset']
            else:
                offset = "0"

            cs = get_snowflake_cursor()
            try:
                cs.execute("use warehouse " + os.environ['SNOWFLAKE_WAREHOUSE'] + ";")
                cs.execute("use schema " + os.environ['SNOWFLAKE_SCHEMA'] + ";")
                cs.execute("select * from table(result_scan('" + query_id + "')) limit 100 offset " + offset + ";")
                #print(','.join([col[0] for col in cs.description]))
                columns = []
                for col in cs.description:
                    columns.append(col[0])
                results = cs.fetchall()
                json_results = []
                for rec in results:
                    json_rec = {}
                    for col in columns:
                        #print('%s: %s' % (col, rec[columns.index(col)]))
                        json_rec[col] = str(rec[columns.index(col)])
                    print(json_rec)
                    json_results.append(json_rec)

                json_root = {}
                json_root['query_id'] = query_id
                json_root['results'] = json_results
                print(json.dumps(json_root))
                message = json.dumps(json_root)

                url = _get_postback_url(event)
                auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], 'us-east-1', 'execute-api', session_token = os.environ['AWS_SESSION_TOKEN'])
                r = requests.post(url, auth=auth, json=json_root)
                print(r.status_code)
            finally:
                cs.close()
        else:
            message = "No query_id provided."

    return _get_response(200, message)
