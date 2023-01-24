import sys, os
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import rts_api as rts
import aiohttp

async def job():
    async with aiohttp.ClientSession() as session:
        print(await rts.async_api_call(session, call_type=rts.API_Call.GET_ROUTES, hash_key=os.getenv('RTS_HASH_KEY'), api_key=os.getenv('RTS_API_KEY')))

### Scheduling Loop ###
def main():
    # Load .env file
    load_dotenv()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, 'interval', seconds=1)
    
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
