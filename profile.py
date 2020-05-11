from invoke import invokeAction, invokeWeb
from multiprocessing import Process, Lock
from collections import OrderedDict
import time
import pandas as pd
import csv
import json

def invokeParallel(i, l_p, l_b):
    name = 'autocomplete/uspresidents'
    params = {'term':'Ge'}
    params = OrderedDict(sorted(params.items()))
    body = {}
    body = OrderedDict(sorted(body.items()))
    t_start = time.time()
    # Check whether can we use reuse buffer
    if USE_REUSE_BUFFER:
        l_b.acquire()
        rb = pd.read_csv('./reuse_buffer_edge.csv')
        matched_item = rb.loc[ (rb['Function Name'] == name) & (rb['Params'] == str(params)) & (rb['Body'] == str(body)) ]
        l_b.release()
        if len(matched_item.index) > 0:
            print(matched_item)
            if len(matched_item.index) > 1:
                print("Two matched items in edge reuse buffer!!")
            return matched_item["Output"]
    res = invokeWeb(name, params, body)
    t_end = time.time()
    l_p.acquire()
    #profile_data.append({'Function Name': name, 'Start Time': t_start, 'End Time': t_end  }, ignore_index=True)
    with open(r'profile_data.csv','a') as fd:
        writer = csv.writer(fd)
        writer.writerow([name, t_start, t_end])
        if USE_REUSE_BUFFER:
            l_b.acquire()
            with open(r'reuse_buffer_edge.csv','a') as rb:
                writer_rb = csv.writer(rb)
                writer_rb.writerow([name, params, body, res])
            l_b.release()

    print(str(i) + res + "\tDuration: " + str(t_end - t_start))
    l_p.release()

profile_data = pd.DataFrame(columns= ['Function Name', 'Start Time', 'End Time'])
profile_data.to_csv(r'./profile_data.csv', index = False)

USE_REUSE_BUFFER = True

if USE_REUSE_BUFFER:
    reuse_buffer = pd.DataFrame(columns= ['Function Name', 'Params', 'Body', 'Output'])
    reuse_buffer.to_csv(r'./reuse_buffer_edge.csv', index = False)

for i in range(10):
    l_p = Lock()
    l_b = Lock()

    t_start = time.time()
    Process(target=invokeParallel, args=(i, l_p, l_b)).start()
    time.sleep(0.1)




