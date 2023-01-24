from apscheduler.schedulers.asyncio import AsyncIOScheduler
import time

import asyncio
import sys, os

async def job():
    # print(await async_api_call(session, call_type=API_Call.GET_ROUTES)
    # async_call()
    await asyncio.sleep(0.3)
    print("yuh")

### Scheduling Loop ###
def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, 'interval', seconds=1)
    
    print("start")
    scheduler.start()

    return scheduler
    
def shutdown(scheduler):
    scheduler.shutdown()

    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(e.code)

if __name__ == "__main__":
    try:
        scheduler = main()
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        # Gracefully shut down on Cltr+C interrupt
        shutdown(scheduler)
    except Exception:
        # Cleanup and reraise. This will print a backtrace.
        raise        
