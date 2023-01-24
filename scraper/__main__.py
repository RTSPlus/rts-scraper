from schedule import every, repeat, run_pending
import time

import sys, os

@repeat(every(1).seconds)
def job():
    print("yhuhhhh")

@repeat(every(2).seconds)
def job2():
    print("yuh2")

def main():
    while True:
        run_pending()
        time.sleep(1)
        print("end")
    
def shutdown():
    print("shutting down")
    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(e.code)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Gracefully shut down on Cltr+C interrupt
        shutdown()
    except Exception:
        # Cleanup and reraise. This will print a backtrace.
        raise        
