from invoke import invokeAction, invokeWeb
from threading import Thread, Lock
from collections import OrderedDict
import time
import pandas as pd
import csv
import string
import random
import os

FUN_PRIMES = 0
FUN_AUTOCOMPLETE = 1
FUN_SENTIMENT = 2

INVOKE_ACTION = 0
INVOKE_WEB = 1

def writeProfileData(name, t_start, t_end, ht):
    global time_start
    with open(r'profile_data.csv','a') as fd:
        writer = csv.writer(fd)
        writer.writerow([name, t_start - time_start, t_end - time_start, ht])
    return

def invokeParallel(i, invoke_type, name, params, body, l_1, l_2):
    params_store = OrderedDict(sorted(params.items()))
    body_store = OrderedDict(sorted(body.items()))

    global rb          # global reuse buffer 
    global time_start  # test start time
    global l_p,l_b
    global hit_count

    t_start = time.time()
    # Check whether can we use reuse buffer
    if USE_REUSE_BUFFER:
        l_b.acquire()
        # rb = pd.read_csv('./reuse_buffer_edge.csv')
        # print(rb)
        matched_item = rb.loc[ (rb['Function Name'] == name) & (rb['Params'] == params_store) & (rb['Body'] == body_store) ]
        l_b.release()
        if len(matched_item.index) > 0:
            # print("Find Matched!")
            if len(matched_item.index) > 1:
                raise Exception("Two matched items in edge reuse buffer!!")
            res = matched_item["Output"]
            t_end = time.time()
            l_p.acquire()
            hit_count += 1
            writeProfileData( name, t_start, t_end, hit_count )
            l_p.release()
            if i % 50 == 0:
                print("Time: " + str(t_start - time_start) +  "  Invocation Time: " + str(t_end - t_start))
                print("Hit Times: " + str(hit_count))
            return res

    if invoke_type == INVOKE_WEB:
        res = invokeWeb(name, params, body)
    elif invoke_type == INVOKE_ACTION:
        res = invokeAction(name, body)
    t_end = time.time()

    # Write profile data
    l_p.acquire()
    writeProfileData( name, t_start, t_end, hit_count)
    l_p.release()

    # Write to reuse buffer
    if USE_REUSE_BUFFER:
        l_b.acquire()
        # Need to judge whether there is matched item again before writing
        matched_item = rb.loc[ (rb['Function Name'] == name) & (rb['Params'] == params_store) & (rb['Body'] == body_store) ]
        if len(matched_item.index) > 0:
            if i % 50 == 0:
                print("Time: " + str(t_start - time_start) +  "  Invocation Time: " + str(t_end - t_start))
                print("Hit Times: " + str(hit_count))
            l_b.release()
            return res

        rb = rb.append({'Function Name': name, 'Params': params_store, 'Body': body_store, 'Output': res} , ignore_index=True)
        '''
        with open(r'reuse_buffer_edge.csv','a') as rb:
	    writer_rb = csv.writer(rb)
	    writer_rb.writerow([name, params, body, res])
        rb = pd.read_csv('./reuse_buffer_edge.csv')
        '''
        # print(rb.memory_usage().sum())
        # Need to judge whether to kick unused items out
        # If the buffer is full, kick the first a few elements out of the buffer
        if (not rb.empty) and rb.memory_usage().sum() > MAX_BUFFER_SIZE:
            idx = 0
            while (not rb.empty) and rb[idx:].memory_usage().sum() > MAX_BUFFER_SIZE:
                idx += 1
            # print("Kick out first " + str(idx) + " rows.")
            rb = rb[idx:]
            '''
	    rb.to_csv(r'./reuse_buffer_edge.csv', index=False)
	    print(pd.read_csv('./reuse_buffer_edge.csv').memory_usage().sum())
            '''
        l_b.release()

    if i % 50 == 0:
        print("Time: " + str(t_start - time_start) +  "  Invocation Time: " + str(t_end - t_start))
        if USE_REUSE_BUFFER:
            print("Hit Times: " + str(hit_count))
    return res

def initPrimes():
    name = 'primes'
    num = random.randint(9999000, 10000000)
    params = {}
    body = {'num': num}
    return name, params, body

def initAutoComplete():
    name = 'autocomplete/uspresidents'
    randstr_tmp = string.ascii_letters
    str_tmp = random.choice(randstr_tmp).upper()
    for i in range(random.randint(0, 2)):
        str_tmp += random.choice(randstr_tmp).lower()
    params = {'term': str_tmp}
    body = {}
    return name, params, body

def initSentiment():
    name = 'sentiment'
    s_nouns = ["A dude", "My mom", "The king", "Some guy", "A cat with rabies", "A sloth", "Your homie", "This cool guy my gardener met yesterday", "Superman"]
    p_nouns = ["These dudes", "Both of my moms", "All the kings of the world", "Some guys", "All of a cattery's cats", "The multitude of sloths living under your bed", "Your homies", "Like, these, like, all these people", "Supermen"]
    s_verbs = ["eats", "kicks", "gives", "treats", "meets with", "creates", "hacks", "configures", "spies on", "retards", "meows on", "flees from", "tries to automate", "explodes"]
    p_verbs = ["eat", "kick", "give", "treat", "meet with", "create", "hack", "configure", "spy on", "retard", "meow on", "flee from", "try to automate", "explode"]
    infinitives = ["to make a pie.", "for no apparent reason.", "because the sky is green.", "for a disease.", "to be able to make toast explode.", "to know more about archeology."]
    str_tmp = random.choice(s_nouns + p_nouns) + ' ' + random.choice(s_verbs) + ' ' + random.choice(s_nouns + p_nouns).lower() + ' ' +random.choice(infinitives)
    body = {'analyse' : str_tmp}
    params = {}
    return name, params, body
    
# Init a random input for function
def randomInit(fun_choice):
    fun_options = {FUN_PRIMES: initPrimes, FUN_AUTOCOMPLETE: initAutoComplete, FUN_SENTIMENT: initSentiment}
    return fun_options[fun_choice]()


profile_data = pd.DataFrame(columns= ['Function Name', 'Start Time', 'End Time', 'Hit Times'])
profile_data.to_csv(r'./profile_data.csv', index = False)

USE_REUSE_BUFFER = True
MAX_BUFFER_SIZE = 256 * 1024
if USE_REUSE_BUFFER:
    print("Max buffer size: " + str(MAX_BUFFER_SIZE/1024) + "K")

global rb
rb = pd.DataFrame(columns= ['Function Name', 'Params', 'Body', 'Output'])
#rb.to_csv(r'./reuse_buffer_edge.csv', index = False)

global hit_count
hit_count = 0

random.seed()
global l_p,l_b
l_p = Lock()
l_b = Lock()

global time_start
time_start = time.time()

ips = 100
for ii in range( 3 ):
    for i in range( ips * 15 ):
        name, params, body = randomInit(FUN_AUTOCOMPLETE)
        #name, params, body = randomInit(FUN_PRIMES)
        #name, params, body = randomInit(FUN_SENTIMENT)

        Thread(target=invokeParallel, args=(i, INVOKE_WEB, name, params, body, l_p, l_b)).start()
        #Thread(target=invokeParallel, args=(i, INVOKE_WEB, name, {'term': 'Ge'}, body, l_p, l_b)).start()
        #Thread(target=invokeParallel, args=(i, INVOKE_ACTION, name, params, body, l_p, l_b)).start()
        #Thread(target=invokeParallel, args=(i, INVOKE_ACTION, name, params, {'num': 100}, l_p, l_b)).start()
        time.sleep(1.0/ips)
    time.sleep(15)



