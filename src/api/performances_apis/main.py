from flask import Flask
from flask import request
# for second-level logging
import logging
from logging.handlers import RotatingFileHandler
import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch

import json

app = Flask(__name__)

# elasticsearch connection - params
ELASTIC_HOST = 'localhost'
ELASTIC_PORT = '9200'

es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

# second-level logging
handler = RotatingFileHandler('logger.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)


@app.errorhandler(400)
def internal_error(error):
    response = app.response_class(
        response='400 CHECKLIST: data-format; data-limitations (i.e. mandatory vs suggested attributes)',
        status=400,
        mimetype='application/json'
    )
    app.logger.error('Logger @ UPPER 400 THROWN ' + str(datetime.datetime.now()))
    return response


@app.errorhandler(404)
def not_found(error):
    response = app.response_class(
        response='404 CHECKLIST: url and eventually uri',
        status=404,
        mimetype='application/json'
    )
    app.logger.error('Logger @ UPPER 404 THROWN ' + str(datetime.datetime.now()))
    return response


@app.route('performances/GetMessage', methods=['GET'])
def retrieve_message():
    """
    Retrieve a single message by specifying the trace id
    :param traceId: the id of the message to retrieve
    :return: the HTTP response
    """

    trace_id = request.args.get('traceId')

    response = es.search(index="mem-ex", body={"query": {"match": {"traceId": trace_id}}})

    if response['hits']['total'] == 0:
        response = app.response_class(
            response='Invalid trace id',
            status=400,
            content_type='text/xml; charset=utf-8'
        )
        return response
    else:
        response = app.response_class(
            response=json.dumps(response['hits']['hits'][0]['_source']),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('performances/GetConversation', methods=['GET'])
def retrieve_conversation():
    """
    Retrieve a conversation by specifying the conversation id
    :param conversationId: the id of the conversation to retrieve
    :return: the HTTP response
    """
    conversation_id = request.args.get('conversationId')

    response = es.search(index="mem-ex", body={"query": {"term": {"coversationId.keyword": {"value": conversation_id}}}})

    if response['hits']['total'] == 0:
        response = app.response_class(
            response='Invalid conversation id',
            status=400,
            content_type='text/xml; charset=utf-8'
        )
        return response
    else:
        response = app.response_class(
            response=json.dumps(response['hits']['hits']),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('performances/GetServicesList', methods=['GET'])
def get_services_message():

    trace_id = request.args.get('trace_id')

    es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

    message = es.search(index="mem-ex", body={"query": {"match": {"traceId": trace_id}}})

    # TODO once some logs arrived, try to understand how to solve this part

    response = app.response_class(
        response=json.dumps({}),
        status=200,
        mimetype='application/json'
    )

    return response


@app.route('performances/GetLowPerformancesModules', methods=['GET'])
def get_low_performances():

    no_days = request.args.get('no_days')

    es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

    # TODO once some logs arrived, modify the query

    messages = es.search(index="mem-ex", body={"query": {"match_all": {}}})

    response = app.response_class(
        response=json.dumps({}),
        status=200,
        mimetype='application/json'
    )

    return response


app.run()
