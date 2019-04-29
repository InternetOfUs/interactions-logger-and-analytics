from flask import Flask
from flask import request
# for second-level logging
import logging
from logging.handlers import RotatingFileHandler
import datetime
# for handling elasticsearch
from elasticsearch import Elasticsearch

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
        response='erroneous data format' ,
        status=400,
        content_type='text/xml; charset=utf-8'
    )
    app.logger.error('Logger @ UPPER 400 THROWN ' + str(datetime.datetime.now()))
    return response


@app.errorhandler(404)
def not_found(error):
    response = app.response_class(
        response='invalid resource',
        status=404,
        content_type='text/xml; charset=utf-8'
    )
    app.logger.error('Logger @ UPPER 404 THROWN ' + str(datetime.datetime.now()))
    return response


@app.route('/LogMessages', methods=['POST'])
def messages():
    """
    Add a batch of messages to the database
    :param messages: a set of messages sa part of the body of the request
    :return: the HTTP response
    """

    # second-level logging
    app.logger.warning('Logger @ start logging an array of messages - ' + str(datetime.datetime.now()))
    # parse the message received
    messages_received = request.json
    # for each key in the dictionary, take the value (the message) and push it in the database
    # there should be just one key associated to a list of messages
    for k, v in messages_received.items():
        # if there is more than one top-level key
        if k != "messages":
            response = app.response_class(
                response='400 CHECKLIST there must be just one top-level key in the json file',
                status=400,
                mimetype='application/json'
            )
            app.logger.warning('Logger @ wrong format of the message (t.l.key) - ' + str(datetime.datetime.now()))
            return response
        for element in v:
            # push the message in the database
            es.index(index='mem-ex', doc_type='message', body=element)

    response = app.response_class(
        response='Ok',
        status=200,
        mimetype='application/json'
    )
    app.logger.warning('Logger @ end logging an array of messages - ' + str(datetime.datetime.now()))
    return response


@app.route('/LogMessage', methods=['POST'])
def message():
    """
    Add a new single message to the database
    :param message: a single message in JSON foramt as part of the body of the request
    :return: the HTTP response
    """
    app.logger.warning('Logger @ start logging of a single message - ' + str(datetime.datetime.now()))

    es.index(index='mem-ex', doc_type='message', body=request.json)

    response = app.response_class(
        response='Ok',
        status=200,
        mimetype='application/json'
    )
    app.logger.warning('Logger @ end logging of a single message - ' + str(datetime.datetime.now()))
    return response


app.run()
