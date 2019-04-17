from flask import Flask
from flask import request
# for second-level logging
import logging
from logging.handlers import RotatingFileHandler
import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch

import requests

import json

app = Flask(__name__)

# elasticsearch connection - params
ELASTIC_HOST = 'localhost'
ELASTIC_PORT = 9200

# second-level logging
handler = RotatingFileHandler('logger.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)

@app.errorhandler(400)
def internal_error(error):
    '''
    function that handle a 400 error and suggest possible solutions

    :param error: the error thrown
    :type error: string

    :output response: a response with status 400 and a checklist for a possible solution
    :type response: response
    '''
    response = app.response_class(
        response='400 CHECKLIST: data-format; data-limitations (i.e. mandatory vs suggested attributes)',
        status=400,
        mimetype='application/json'
    )
    app.logger.error('Logger @ UPPER 400 THROWN ' + str(datetime.datetime.now() ))
    return response

@app.errorhandler(404)
def not_found(error):
    '''
    function that handle a 404 error and suggest possible solutions

    :param error: the error thrown
    :type error: string

    :output response: a response with status 403 and a checklist for a possible solution
    :type response: response
    '''
    response = app.response_class(
        response='404 CHECKLIST: url and eventually uri',
        status=404,
        mimetype='application/json'
    )
    app.logger.error('Logger @ UPPER 404 THROWN ' + str(datetime.datetime.now() ))
    return response

@app.route('/messages', methods=['POST'])
def messages():
    '''
    to log an array of messages into the elasticsearch instance

    :param json: a valid json string in the body of the request
    :type json: string

    :output response: a response with a status and, eventually, a checklist
    :type response: response
    '''
    # second-level logging
    app.logger.warning('Logger @ start logging an array of messages - ' + str(datetime.datetime.now() ))
    # open a connection with elasticsearch
    es=Elasticsearch([{'host':ELASTIC_HOST,'port':ELASTIC_PORT}])
    # parse the message received
    messages_received = request.json
    # for each key in the dictionary, take the value (the message) and push it in the database
    # there should be just one key associated to a list of messages
    for k,v in messages_received.items():
        # if there is more than one top-level key
        if(len(k) > 1):
            response = app.response_class(
                response='400 CHECKLIST there must be just one top-level key in the json file (i.e. {messages:[{...},...]})',
                status=400,
                mimetype='application/json'
            )
            app.logger.warning('Logger @ wrong format of the message (tlkey) - ' + str(datetime.datetime.now() ))
            return response
        for element in v:
            # push the message in the database
            es.index(index='mem-ex',doc_type='message',body=element)
    # eventaully return with a 200 state
    response = app.response_class(
        response='Ok',
        status=200,
        mimetype='application/json'
    )
    app.logger.warning('Logger @ end logging an array of messages - ' + str(datetime.datetime.now() ))
    return response

@app.route('/message', methods=['POST'])
def message():
    '''
    to log an single message into the elasticsearch instance

    :param json: a valid json string in the body of the request
    :type json: string

    :output response: a response with a status and, eventually, a checklist
    :type response: response
    '''
    app.logger.warning('Logger @ start logging of a single message - ' + str(datetime.datetime.now() ))

    es=Elasticsearch([{'host':ELASTIC_HOST,'port':ELASTIC_PORT}])

    es.index(index='mem-ex',doc_type='message',body=request.json)

    response = app.response_class(
        response='Ok',
        status=200,
        mimetype='application/json'
    )
    app.logger.warning('Logger @ end logging of a single message - ' + str(datetime.datetime.now() ))
    return response
