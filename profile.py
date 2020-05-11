from invoke import invokeAction, invokeWeb
#from concurrent import futures
from multiprocessing import Process
import time

def invokeParallel(index):
   #res = invoke('helloPython', {'name': 'Kaifeng',})
   res = invokeWeb('autocomplete/uspresidents', {'term':'Ge'}, {})
   print(str(index) + res)

#with futures.ProcessPoolExecutor() as pool:
for i in range(10):
    #res = pool.submit(invokeParallel, i)
    Process(target=invokeParallel, args=(i,)).start()
    time.sleep(0.1)



