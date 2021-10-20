from flask import (
    Blueprint, flash, g, redirect, render_template, session, Response, send_from_directory, request, url_for
)
from werkzeug.exceptions import abort
import os
from flask.json import jsonify
from time import time
import traceback
from multiprocessing import Process, Queue, active_children
import hashlib
import json
import logging

from awudima import AwudimaFQP, Federation, DataSourceType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

bp = Blueprint('query', __name__, url_prefix='/')


if 'CONFIG_FILE' in os.environ:
    configfile = os.environ['CONFIG_FILE']
else:
    configfile = '/data/federation.json'


@bp.route("/sparql", methods=['POST', 'GET'])
def sparql():
    if request.method == 'GET' or request.method == 'POST':
        try:
            query = request.args.get("query", '')

            if os.path.exists(configfile):
                federation = Federation.load_from_json(configfile)
            else:
                return jsonify({
                                    "head": {
                                        "vars": []
                                    },
                                    "results": {
                                        "bindings": []
                                    },
                                    'message': 'Error in executing the query!',
                                    'query': query,
                                    "error": "Federation setting is not found as '/data/federation.json'"})
            fqp = AwudimaFQP(federation)
            resultset = fqp.execute(query, keep_in_memory=True)
            if resultset:
                return jsonify(resultset.results)
            else:
                return jsonify({
                                    "head": {
                                        "vars": []
                                    },
                                    "results": {
                                        "bindings": []
                                    },
                                    'message': 'Error in executing the query!',
                                    'query': query,
                                    'error': 'Exception while executing the query!'
                                })
        except Exception as e:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            emsg = repr(traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback))
            logger.error("Exception  " + emsg)
            print("Exception: ", e)
            import pprint
            pprint.pprint(emsg)
            return jsonify({
                                    "head": {
                                        "vars": []
                                    },
                                    "results": {
                                        "bindings": []
                                    },
                                    'message': 'Error in executing the query!',
                                    'query': request.args.get("query", 'No SPARQL query found'),
                                    "error": str(emsg)
                    })

    else:
        return jsonify({
                        "head": {
                            "vars": []
                        },
                        "results": {
                            "bindings": []
                        },
                        'message': 'Error in executing the query!',
                        'query': 'No SPARQL query found',
                        "error": "Invalid HTTP method used. Use GET "}
                )


@bp.route("/configure", methods=['POST', 'GET'])
def configure():
    if request.method == 'GET' or request.method == 'POST':
        try:
            conf = request.args.get("federation", {})
            fed = json.loads(conf)
            federation = Federation.config(fed)
            federation.extract_molecules()
            federation.dump_to_json(configfile)
            return jsonify({'status': True, 'federation': federation.to_json()})
        except Exception as e:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            emsg = repr(traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback))
            logger.error("Exception  " + emsg)
            print("Exception: ", e)
            import pprint
            pprint.pprint(emsg)
            return jsonify({'status': False,
                            "error": str(emsg)
                            })

    else:
        return jsonify({'status': False,
                        "error": "Invalid HTTP request! Only GET and POST are supported!"
                            })


@bp.route("/inspect", methods=['POST', 'GET'])
def inspect():
    if request.method == 'GET' or request.method == 'POST':
        if not os.path.exists(configfile):
            return jsonify({'federation': None})
        try:
            federation = Federation.load_from_json(configfile)
            return jsonify({'federation': federation.to_json()})
        except Exception as e:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            emsg = repr(traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback))
            logger.error("Exception  " + emsg)
            print("Exception: ", e)
            import pprint
            pprint.pprint(emsg)
            return jsonify({'federation': None,
                            "error": str(emsg)
                            })
    else:
        return jsonify({'federation': None,
                        "error": "Invalid HTTP request! Only GET and POST are supported!"
                     })
