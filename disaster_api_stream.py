import asyncio
import aiohttp
import reactivex
from reactivex import operators as ops, Observable
import disaster_saver


async def fetch_data():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://eonet.gsfc.nasa.gov/api/v3/events') as resp:
            return await resp.json()


def main(current_loop):
    source: Observable[int] = reactivex.interval(5)  # type: ignore

    def flatten(i):
        print(i)
        return reactivex.from_future(asyncio.run_coroutine_threadsafe(fetch_data(), loop=current_loop))

    source.pipe(
        ops.flat_map(flatten),  # do async API request
        ops.map(lambda x: x['events']),  # from API response get list with events
        ops.flat_map(lambda x: x),  # flatten events list
        ops.map(lambda i: (i['id'], i['title'], i['geometry'][-1]['coordinates'][0], i['geometry'][-1]['coordinates'][1])),  # get only needful attributes from event
        lambda x: disaster_saver.save(x)
    ).subscribe(on_next=print)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    main(loop)
    loop.run_forever()
