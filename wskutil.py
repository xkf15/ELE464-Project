"""Whisk Utility methods.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""
# Modified by Kaifeng Xu from original wskutil


import os
import json
import sys
if sys.version_info.major >= 3:
    from http.client import HTTPConnection, HTTPSConnection, IncompleteRead
    from urllib.parse import urlparse
else:
    from httplib import HTTPConnection, HTTPSConnection, IncompleteRead
    from urlparse import urlparse
import ssl
import base64
import socket

import time
import pandas as pd 
from threading import Lock
import random
global rb
global hit_count
rb = pd.DataFrame(columns= ['Url', 'Body', 'Output'])
l = Lock()
hit_count = 0

USE_REUSE_BUFFER = True
MAX_BUFFER_SIZE = 1 * 1024 * 1024

# global configurations, can control whether to allow untrusted certificates
# on HTTPS connections
httpRequestProps = {'secure': False}

def request(method, urlString, body = '', headers = {}, auth = None, verbose = False, https_proxy = os.getenv('https_proxy', None), timeout = 60):
    url = urlparse(urlString)
    if url.scheme == 'http':
        conn = HTTPConnection(url.netloc, timeout = timeout)
    else:
        if httpRequestProps['secure'] or not hasattr(ssl, '_create_unverified_context'):
            conn = HTTPSConnection(url.netloc if https_proxy is None else https_proxy, timeout = timeout)
        else:
            conn = HTTPSConnection(url.netloc if https_proxy is None else https_proxy, context=ssl._create_unverified_context(), timeout = timeout)
        if https_proxy:
            conn.set_tunnel(url.netloc)

    if auth is not None:
        auth = base64.b64encode(auth.encode()).decode()
        headers['Authorization'] = 'Basic %s' % auth

    if verbose:
        print('========')
        print('REQUEST:')
        print('%s %s' % (method, urlString))
        print('Headers sent:')
        print(getPrettyJson(headers))
        if body != '':
            print('Body sent:')
            print(body)

    try:
        # Add random network delay
        time.sleep(random.expovariate(40))

        conn.request(method, urlString, body, headers)

        # read reuse buffer
        global rb
        body_store = body
        if USE_REUSE_BUFFER:
            l.acquire()
            matched_item = rb.loc[ (rb['Url'] == urlString) & (rb['Body'] == body_store) ]
            l.release()
            if len(matched_item.index) > 0:
                # print("Find Matched!")
                if len(matched_item.index) > 1:
                    raise Exception("Two matched items in edge reuse buffer!!")
                res = matched_item["Output"]
                t_end = time.time()
                global hit_count
                hit_count += 1
                if hit_count % 20 == 0:
                    print("Hit Times: " + str(hit_count))
                return res

        # Get response
        res = conn.getresponse()
        body = ''
        try:
            body = res.read()
        except IncompleteRead as e:
            body = e.partial

        # patch the read to return just the body since the normal read
        # can only be done once
        res.read = lambda: body

        if verbose:
            print('--------')
            print('RESPONSE:')
            print('Got response with code %s' % res.status)
            print('Body received:')
            print(res.read())
            print('========')

        # store Reuse Buffer
        if USE_REUSE_BUFFER:
            l.acquire()
            # Need to judge whether there is matched item again before writing
            matched_item = rb.loc[ (rb['Url'] == urlString) & (rb['Body'] == body_store) ]
            if len(matched_item.index) > 0:
                l.release()
                return res.read().decode()

            rb = rb.append({'Url': urlString, 'Body': body_store, 'Output': res.read().decode()} , ignore_index=True)
            # Need to judge whether to kick unused items out
            # If the buffer is full, kick the first a few elements out of the buffer
            if (not rb.empty) and rb.memory_usage().sum() > MAX_BUFFER_SIZE:
                idx = 0
                while (not rb.empty) and rb[idx:].memory_usage().sum() > MAX_BUFFER_SIZE:
                    idx += 1
                rb = rb[idx:]
            l.release()

        return res.read().decode()
    except socket.timeout:
        return ErrorResponse(status = 500, error = 'request timed out at %d seconds' % timeout)
    except Exception as e:
        return ErrorResponse(status = 500, error = str(e))


def getPrettyJson(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


# class to normalize responses for exceptions with no HTTP response for canonical error handling
class ErrorResponse:
    def __init__(self, status, error):
        self.status = status
        self.error = error

    def read(self):
        return self.error
