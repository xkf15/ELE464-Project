#import requests
from wskutil import request
import json

APIHOST = 'https://localhost'
AUTH_KEY = '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'
NAMESPACE = '_'
ACTION = ''
BODY = {};
BLOCKING = 'true'
RESULT = 'true'
HEADERS = {'Content-Type': 'application/json',}

def setConfig():
    APIHOST = 'https://localhost'
    AUTH_KEY = '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'
    NAMESPACE = '_'
    BLOCKING = 'true'
    RESULT = 'true'
    HEADERS = {'Content-Type': 'application/json',}

#print(base64.b64encode(AUTH_KEY.encode()).decode())
#print(getConfig())

def invoke(name, body):
    setConfig()
    ACTION = name
    BODY = body
    param_str = 'blocking=' + BLOCKING + '&&result=' + RESULT
    url = APIHOST + '/api/v1/namespaces/' + NAMESPACE + '/actions/' + ACTION + '?'  + param_str
    response = request('POST', url, body=json.dumps(BODY), headers=HEADERS, auth=AUTH_KEY)
    #response = requests.post(url, json=PARAMS, params={'blocking': BLOCKING, 'result': RESULT}, headers=HEADERS, verify=False)
    print(response)

