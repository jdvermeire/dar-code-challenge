import json
import asyncio
import websockets


MOCK_EV_URI = 'wss://ws-feed-public.sandbox.pro.coinbase.com'

async def hello():
	
	uri = MOCK_EV_URI
	print("connecting to coinbase websocket...")
	async with websockets.connect(uri) as ws:
		print("connected.")

		print("sending subscription...")
		await ws.send(mock_subscribe())
		print("sent.")

		
		for i in range(30):
			print("receiving response...")
			resp = await ws.recv()
			print(resp)

		print('closing...')



def mock_subscribe():
	return json.dumps({
		"type": "subscribe",
		"product_ids": [
			"BTC-USD"
		],
		"channels": [
			"heartbeat",
			"full"
		]
	})

asyncio.get_event_loop().run_until_complete(hello())