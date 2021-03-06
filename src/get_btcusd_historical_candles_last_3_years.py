import os
import sys
import asyncio
import time

from bfxapi import Client

bfx = Client(
  logLevel='DEBUG',
)

# now = int(round(time.time() * 1000))
# then = now - (1000 * 60 * 60 * 24 * 10) # 10 days ago

async def run():
  end = 1656259200 * 1000   # 2022-06-27
  start = 1483200000 * 1000   # 2017-01-01
  fd = open('./data/historical_candles_last_3_years.csv', 'a+')
  while start < end:
    candles = await bfx.rest.get_public_candles('tBTCUSD', start=start, end=start + 24 * 60 * 60 * 30 * 1000, section='hist', tf='1h', limit=10000, sort=1)
    if candles:
      start = start + 24 * 60 * 60 * 30 * 1000
      print ("get candles: ", len(candles))
      [ fd.write(','.join(map(lambda x: str(x), c)) + '\n') for c in candles ]
      time.sleep(5)
  fd.close()

asyncio.run(run())