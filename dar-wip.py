import json
# import ssl
import asyncio
import datetime
import websockets

from pandas import DataFrame

# TODO add logging


## MOCK ENVIRONMENT VARIABLES ##

# base uri to connect to coinbase pro websocket
# MOCK_EV_URI = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
MOCK_EV_URI = 'wss://ws-feed.pro.coinbase.com'

# list of products to track
MOCK_EV_PRODUCTS = ["BTC-USD", "BTC-EUR"]

# interval, in seconds, to send data to CSV
MOCK_EV_EXPORT_INTERVAL = 600

# interval, in seconds, to run app
MOCK_EV_RUN_INTERVAL = 120

# inverval, in seconds, to aggregate volume weighted average
MOCK_EV_VWA_INTERVAL = 60

# directory to dump files
MOCK_EV_DUMP_PATH = 'data/'


## HANDLERS ##
# TODO break message handlers into their own classes and files

# TODO make this part of a more generalized class?
class Coinbase(object):

	# TODO move these?
	queues = {}

	def __init__(
		self,
		websocket,
		export_seconds,
		run_seconds,
		vwa_seconds,
		*args, 
		**kwargs
	):
		# TODO clean up unnecessary params
		self.ws = websocket
		self.start_time = datetime.datetime.now()
		self.run_interval = datetime.timedelta(seconds=run_seconds)
		self.export_interval = datetime.timedelta(seconds=export_seconds)
		self.vwa_interval = datetime.timedelta(seconds=vwa_seconds)
		self.end_time = self.start_time + self.run_interval
		self.next_dump_time = self.start_time + self.export_interval


	async def subscribe(self, products, channels):
		"""
		Sends subscription message to Coinbase websocket connection


		Parameters
		----------
		products : list
			a list of coinbase product_ids
		channels : list
			list of coinbase websocket channels
		"""
		await self.ws.send(json.dumps({
			"type": "subscribe",
			"product_ids": products,
			"channels": channels
		}))

		# TODO make this more elegant, this is rather brute force
		# setup channel queues
		if 'heartbeat' in channels:
			if 'heartbeat' not in self.queues:
				self.queues["heartbeat"] = []

		if 'status' in channels:
			if 'status' not in self.queues:
				self.queues["status"] = []

		if 'ticker' in channels:
			if 'ticker' not in self.queues:
				self.queues["ticker"] = []


		if 'level2' in channels:
			if 'snapshot' not in self.queues:
				self.queues["snapshot"] = []

			if 'l2update' not in self.queues:
				self.queues["l2update"] = []


		if 'matches' in channels:
			if 'match' not in self.queues:
				self.queues["match"] = []


		if 'full' in channels or 'user' in channels:
			if 'received' not in self.queues:
				self.queues["received"] = []

			if 'open' not in self.queues:
				self.queues["open"] = []

			if 'done' not in self.queues:
				self.queues["done"] = []

			if 'match' not in self.queues:
				self.queues["match"] = []

			if 'change' not in self.queues:
				self.queues["change"] = []

			if 'activate' not in self.queues:
				self.queues["activate"] = []


	async def message_handler(self, message):
		"""
		Handles incoming messages from the websocket and redirects them to the appropriate handler


		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		handlers = {
			"heartbeat": self.heartbeat_handler,
			"snapshot": self.snapshot_handler,
			"l2update": self.l2update_handler,
			"status": self.status_handler,
			"ticker": self.ticker_handler,
			"received": self.received_handler,
			"open": self.open_handler,
			"done": self.done_handler,
			"match": self.match_handler,
			"last_match": self.match_handler,
			"change": self.change_handler,
			"activate": self.activate_handler,
			"subscriptions": self.subscriptions_handler,
			"error": self.error_handler,
		}

		if message["type"] not in handlers:
			# TODO define a formal exception type
			raise Exception("Invalid message type")

		handler = handlers[message["type"]]
		await handler(message)


	# TODO clean these methods up. used brute force for simplicity.
	async def heartbeat_handler(self, message):
		"""
		Handler for heartbeat messages

		Parameters
		----------
		message : dict
			a heartbeat message from a websocket connection
		"""
		# DEBUG
		# print(message)
		pass


	async def snapshot_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def l2update_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def status_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def ticker_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		self.queues["ticker"].append(message)


	async def received_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def open_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def done_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def match_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		self.queues["match"].append(message)



	async def change_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def activate_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def subscriptions_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	async def error_handler(self, message):
		"""

		Parameters
		----------
		message : dict
			a message received from a websocket connection
		"""
		pass


	def clear_queue(self, queue_name):
		"""
		Clears a named queue

		Parameters
		----------
		queue_name : str
			name of the message queue to clear
		"""
		self.queues[queue_name] = []
		


async def run():
	
	uri = MOCK_EV_URI

	print("connecting to coinbase websocket...")

	# TODO add error handling here with a try/catch block
	#      allow for websocket to reconnect on timeout
	async with websockets.connect(uri) as ws:
		print("connected.")

		ch = Coinbase(
			websocket = ws,
			# TODO remove unnecessary params
			run_seconds=MOCK_EV_RUN_INTERVAL,
			export_seconds=MOCK_EV_EXPORT_INTERVAL,
			vwa_seconds=MOCK_EV_VWA_INTERVAL,
			dump_path=MOCK_EV_DUMP_PATH,
		)

		# using the "matches" channel as it streams completed portions of orders
		await ch.subscribe(products=MOCK_EV_PRODUCTS, channels=["heartbeat", "matches"])
		
		while datetime.datetime.now() < ch.end_time:
			
			resp = await ws.recv()

			await ch.message_handler(json.loads(resp))

		print('closing...')

	df = DataFrame(ch.queues["match"])
	df = calculate_table(df)
	print(df.columns)
	print(df.dtypes)
	print(df)
	# print(ch.ticker_queue)


def calculate_table(match_queue_df):
	"""
	Return table with minute-by-minute volume weighted average price
	"""
	df = DataFrame()
	df["product"] = match_queue_df["product_id"]
	df["time"] = match_queue_df["time"].apply(lambda dt: datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ'))

	# truncate time to minute
	df["time"] = df["time"].apply(lambda dt: datetime.datetime(
		year=dt.year,
		month=dt.month,
		day=dt.day,
		hour=dt.hour,
		minute=dt.minute
	))
	df["price"] = match_queue_df["price"].apply(lambda prc: float(prc))
	df["size"] = match_queue_df["size"].apply(lambda sz: float(sz))
	df["volume_price"] = df["price"] * df["size"]

	df = df.groupby(by=["product", "time"]).sum()

	df["vwa_price"] = df["volume_price"] / df["size"]

	df = df.drop(columns = ["price", "size", "volume_price"])

	return df



asyncio.get_event_loop().run_until_complete(run())