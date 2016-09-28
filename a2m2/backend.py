#!/usr/bin/python

# Copyright 2016 Shakir James. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

"""Backend for Image search"""
from __future__ import print_function

import json
import logging

from flask import Flask, request, Response
from a2m1.kvs import DynamoDB, Shelf
from a2m1.loader import IMAGES_KVS_NAME, TERMS_KVS_NAME
from a2m1.querier import query

app = Flask(__name__, static_folder='frontend')

# images_kvs = DynamoDB(IMAGES_KVS_NAME)
# terms_kvs = DynamoDB(TERMS_KVS_NAME)
images_kvs = Shelf('a2m1/' + IMAGES_KVS_NAME)
terms_kvs = Shelf('a2m1/' + TERMS_KVS_NAME)

logging.basicConfig(level=logging.DEBUG)


@app.route('/api/search', methods=['GET'])
def search(images_kvs=images_kvs, terms_kvs=terms_kvs):
    """Return JSON search results

    The response format
    {
      "searchInformation": {
        "totalResults": integer
      }
      "items": [
        {
          "link": string
        }
      ]
    }
    """
    terms = request.args.get('t')
    num = int(request.args.get('num', 100))
    matches = []  # TODO matches = query()
    results = {}  # TODO build from matches
    #
    # TODO
    #
    return Response(json.dumps(results), mimetype='application/json')


@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/<string:filename>')
def send_static(filename):
    return app.send_static_file(filename)


if __name__ == "__main__":
    app.run()
