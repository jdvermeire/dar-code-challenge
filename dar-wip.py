import json
# import ssl
import asyncio
import requests
import datetime
import websockets

from pandas import DataFrame

# TODO add logging
# TODO add exchange rate from: https://api.exchangeratesapi.io/latest?symbols=USD


## MOCK ENVIRONMENT VARIABLES ##

# uri for EUR exchange rates
MOCK_EV_EXCHANGE_URI = 'https://api.exchangeratesapi.io/latest'

# list of currencies to return EUR exchange rate
MOCK_EV_EXCHANGE_SYMBOLS = ['USD']

# base uri to connect to coinbase pro websocket
# MOCK_EV_URI = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
MOCK_EV_URI = 'wss://ws-feed.pro.coinbase.com'

# list of products to track
MOCK_EV_PRODUCTS = ["BTC-USD", "BTC-EUR"]

# interval, in seconds, to send data to CSV
MOCK_EV_EXPORT_INTERVAL = 20

# interval, in seconds, to run app
MOCK_EV_RUN_INTERVAL = 120

# inverval, in seconds, to aggregate volume weighted average
MOCK_EV_VWA_INTERVAL = 60

# timestamp format string
MOCK_EV_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

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
		*args, 
		**kwargs
	):
		# TODO clean up unnecessary params
		self.ws = websocket


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
		print(message)
		# pass


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
		Handle match type messages from coinbase websocket channel

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
		

# -------------------------------------------------------------------


def get_eur_rates(symbols):
	"""
	Return a dict of EUR exchange rates for the input currency symbols

	Parameters
	----------
	symbols : list
		a list of currency symbols
	"""

	resp = requests.get(
		MOCK_EV_EXCHANGE_URI,
		params={
			"symbols": ','.join(symbols)
		}
	)

	return resp.json()["rates"]


# -------------------------------------------------------------------

def calculate_table(match_queue_df):
	"""
	Return table with minute-by-minute volume weighted average price

	Parameters
	----------
	match_queue_df : DataFrame
		a DataFrame of the match queue from coinbase
	"""
	# get the current exchange rate for USD
	eur_usd = get_eur_rates(["USD"])["USD"]

	df = DataFrame()
	df["product"] = match_queue_df["product_id"]
	df["time"] = match_queue_df["time"].apply(lambda dt: datetime.datetime.strptime(dt, MOCK_EV_TIMESTAMP_FORMAT))

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

	# convert EUR to USD
	df.loc[df["product"] == 'BTC-EUR', ["price"]] *= eur_usd

	df["volume_price"] = df["price"] * df["size"]

	df = df.groupby(by=["product", "time"]).sum()

	df["vwa_price"] = df["volume_price"] / df["size"]

	df = df.drop(columns = ["price", "size", "volume_price"])

	return df

# -------------------------------------------------------------------

def dump_vwa(cb, path='', as_of=datetime.datetime.now()):
	"""
	Cuts and dumps the volume weighted average price from a coinbase match queue

	Parameters
	----------
	cb : Coinbase
		Coinbase object that holds the match queue
	path : str
		Directory to save export file
	as_of : datetime
		Datetime at which to cut the match queue
	"""
	if path.endswith('/'):
		path = path.rstrip('/')

	print('length of match queue: %s' % len(cb.queues["match"]))
	print(datetime.datetime.strptime(cb.queues["match"][0]["time"], MOCK_EV_TIMESTAMP_FORMAT))
	print(as_of)

	cut_queue = [
		rec 
		for rec 
		in cb.queues["match"] 
		if datetime.datetime.strptime(rec["time"], MOCK_EV_TIMESTAMP_FORMAT) <= as_of
	]

	print('length of cut queue: %s' % len(cut_queue))

	if not len(cut_queue):
		return

	df = DataFrame(cut_queue)

	# get calculated dataframe to export
	df = calculate_table(df)

	df.to_csv('%s/match_%s.csv' % (path, as_of.strftime("%Y%m%d%H%M%S")))

	# reset queue
	# TODO there's likely a more performant way to do this, but this works for now
	cb.queues["match"] = [
		rec
		for rec
		in cb.queues["match"]
		if datetime.datetime.strptime(rec["time"], MOCK_EV_TIMESTAMP_FORMAT) > as_of
	]

# -------------------------------------------------------------------

async def run():
	
	# get environment vars
	uri = MOCK_EV_URI
	
	exchange_uri = MOCK_EV_EXCHANGE_URI
	exchange_symbols = MOCK_EV_EXCHANGE_SYMBOLS

	run_seconds = MOCK_EV_RUN_INTERVAL
	export_seconds = MOCK_EV_EXPORT_INTERVAL
	# vwa_seconds = MOCK_EV_VWA_INTERVAL	# Not going to use this here, not sure on the implementation
	dump_path = MOCK_EV_DUMP_PATH

	# initial run vars
	start_time = datetime.datetime.utcnow()
	run_interval = datetime.timedelta(seconds=run_seconds)
	export_interval = datetime.timedelta(seconds=export_seconds)
	# vwa_interval = datetime.timedelta(seconds=vwa_seconds)
	end_time = start_time + run_interval
	next_dump_time = datetime.datetime(
		year=start_time.year,
		month=start_time.month,
		day=start_time.day,
		hour=start_time.hour,
		minute=start_time.minute) + export_interval

	print("connecting to coinbase websocket...")

	# TODO add error handling here with a try/catch block
	#      allow for websocket to reconnect on timeout
	async with websockets.connect(uri) as ws:
		print("connected.")

		ch = Coinbase(
			websocket = ws,
		)

		# using the "matches" channel as it streams completed portions of orders
		await ch.subscribe(products=MOCK_EV_PRODUCTS, channels=["heartbeat", "matches"])
		
		while datetime.datetime.utcnow() < end_time:

			resp = await ws.recv()

			await ch.message_handler(json.loads(resp))

			if datetime.datetime.utcnow() > next_dump_time:
				dump_vwa(ch, path=dump_path, as_of=next_dump_time)
				print('current dump time: %s' % next_dump_time)

				next_dump_time += export_interval

				print('next dump time: %s' % next_dump_time)

		print('closing...')

		# dump remainder of queue
		if len(ch.queues["match"]):
			dump_vwa(ch, path=dump_path)


asyncio.get_event_loop().run_until_complete(run())