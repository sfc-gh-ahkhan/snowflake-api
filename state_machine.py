import json
import snowflake.connector
import urllib.parse
import requests
from requests_aws4auth import AWS4Auth
import os

def _get_snowflake_cursor():
    ctx = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT']
        )
    return ctx.cursor()

def start_run (event, context):
    # if 'body' in event:
    #     body = json.loads(event['body'])

    if 'view_name' in event:
        snowflake_view = event['view_name']
        print("now running snowflake view: ", snowflake_view)

        cs = _get_snowflake_cursor()
        try:
            print("starting to execute query")
            cs.execute("use warehouse " + os.environ['SNOWFLAKE_WAREHOUSE'] + ";")
            cs.execute("use schema " + os.environ['SNOWFLAKE_SCHEMA'] + ";")
            cs.execute("SELECT * from " + snowflake_view, _no_results=True)
            query_id = cs.sfqid
            print("query id: ", query_id)

            if 'post_back_url' in event:
                url = event['post_back_url']
                print("now trying to post back to: ", url)
                data = "Now running query_id: " + query_id
                auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], 'us-east-1', 'execute-api', session_token = os.environ['AWS_SESSION_TOKEN'])
                r = requests.post(url, auth=auth, data=data)
        finally:
            pass
            #cs.close()

        return query_id


def get_execution_status (event, context):
    if 'query_id' in event:
        query_id = event['query_id']
        cs = _get_snowflake_cursor()
        try:
            cs.execute("use warehouse " + os.environ['SNOWFLAKE_WAREHOUSE'] + ";")
            cs.execute("use schema " + os.environ['SNOWFLAKE_SCHEMA'] + ";")
            cs.execute("select execution_status from table(information_schema.query_history()) where query_id like '" + query_id + "';")
            status = cs.fetchone()[0]
            #print(one_row[0])
        finally:
            cs.close()

        return status

def post_back_results (event, context):
    if 'query_id' in event:
        query_id = event['query_id']

        cs = _get_snowflake_cursor()
        try:
            cs.execute("use warehouse " + os.environ['SNOWFLAKE_WAREHOUSE'] + ";")
            cs.execute("use schema " + os.environ['SNOWFLAKE_SCHEMA'] + ";")
            cs.execute("select * from table(result_scan('" + query_id + "')) limit 100;")
            results = cs.fetchall()
            print(results)
        finally:
            cs.close()

        columns = []
        for col in cs.description:
            columns.append(col[0])
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
        #print(json.dumps(json_root))

        if 'post_back_url' in event:
            url = event['post_back_url']
            print("now trying to post back to: ", url)
            auth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], 'us-east-1', 'execute-api', session_token = os.environ['AWS_SESSION_TOKEN'])
            r = requests.post(url, auth=auth, json=json_root)
            print(r.status_code)


        return results
