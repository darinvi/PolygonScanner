# from multiprocesses import MultiProcessing
# import multiprocessing as mp

# if __name__ == '__main__':
#     queue = mp.Queue()

#     ws_process = mp.Process(target=MultiProcessing.run_ws, args=(queue,))
#     threading_process = mp.Process(target=MultiProcessing.run_threading, args=(queue,))

#     ws_process.start()
#     threading_process.start()

#     ws_process.join()
#     threading_process.join()
