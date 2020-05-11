from invoke import invokeAction, invokeWeb
from multiprocessing import Process, Lock
import time
import pandas as pd
import csv

profile_data = pd.DataFrame(columns= ['Function Name', 'Start Time', 'End Time'])
profile_data.to_csv(r'./profile_data.csv', index = False)

def invokeParallel(i, l):
   name = 'autocomplete/uspresidents'
   t_start = time.time()
   res = invokeWeb(name, {'term':'Ge'}, {})
   t_end = time.time()
   l.acquire()
   #profile_data.append({'Function Name': name, 'Start Time': t_start, 'End Time': t_end  }, ignore_index=True)
   with open(r'profile_data.csv','a') as fd:
       writer = csv.writer(fd)
       writer.writerow([name, t_start, t_end])
   print(str(i) + res + "\tDuration: " + str(t_end - t_start))
   l.release()
   if i == 9:
       print(pd.read_csv('profile_data.csv'))

for i in range(10):
    l = Lock()

    t_start = time.time()
    Process(target=invokeParallel, args=(i,l)).start()




