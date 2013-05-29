import zmq
import gzip
import time
import logging
import cStringIO

from json import dumps
from datetime import datetime
from multiprocessing import Process

class Streamer(Process):

	def __init__(self, prefix, partition_keys, partition_values, granularity, extension, use_dir, use_file, log_level):

		self.prefix = prefix
		self.partition_keys = partition_keys or []
		self.partition_values = partition_values or []
		self.granularity = granularity
		self.extension = extension
		self.use_dir = use_dir
		self.use_file = use_file

		if granularity:
			grans = ['year','month','day','hour','minute','second']
			self.granularities = grans[:grans.index(granularity) + 1]
		else:
			self.granularities = []

		self.max_buffer_size = 6e6 # 6 MB
		#self.max_buffer_size = 100 # 6 MB
		self.num_messages = 0
		self.name = self.create_name()
		self.log = logging.getLogger("%s" % self.name)
		self.log.setLevel(log_level)
		self.log.debug("Streamer for %s", self.name)
		super(Streamer, self).__init__()


	def create_name(self):
		parts = []
		if self.prefix:
			parts.append(self.prefix)
		parts.extend(['%s' % p for p in self.partition_values])
		return "-".join(parts)


	def get_partition_name(self, now):
		dir_parts = []
		file_parts = []
		if self.prefix:
			dir_parts.append(self.prefix)
			file_parts.append(self.prefix)
		if self.partition_keys:
			dir_parts.append('/'.join(['%s=%s' % (p[0], p[1]) for p in zip(self.partition_keys, self.partition_values)]))
			file_parts.append('-'.join(['%s' % p for p in self.partition_values]))
		if self.granularities:
			period_values = [now.__getattribute__(g) for g in self.granularities]
			dir_parts.append('/'.join(['%s=%s' % (k,v) for k,v in zip(self.granularities, period_values)]))
			file_parts.append(''.join(['%02d' % (v) for v in period_values]))
		if dir_parts:
			dir_parts.append('')

		if self.use_dir:
			dir_name = '/'.join(dir_parts)
		else:
			dir_name = ''

		if self.use_file:
			file_name = '-'.join(file_parts)
		else:
			if self.prefix:
				file_name = self.prefix
			else:
				file_name = 'out'

		partition_name = "%s%s%s" % (dir_name, file_name, self.extension)
		return partition_name


	#def start_new_mpu(self, now):
		#pass


	def create_new_buffer(self):
		self.sio = cStringIO.StringIO()
		self.gzf = gzip.GzipFile(fileobj=self.sio, mode='wb')
		self.log.debug("New gzipped buffer created")


	def write_to_buffer(self, data):
		self.gzf.write(data + "\n")
		self.num_messages += 1
		self.log.debug("num_messages: %d, gzf_size: %d, sio_size: %d", self.num_messages, self.gzf.tell(), self.sio.tell())


	def get_buffer_to_upload(self, last_part=False):
		# If this is the last part of the mpu, close the gzip file
		if last_part:
			self.gzf.close()
		# What's the current position in the buffer?
		s = self.sio.tell()
		# Go to the beginning of the buffer, because that is where we want to start reading
		self.sio.reset()
		# Copy until the position we have written to
		sio_upload = cStringIO.StringIO(self.sio.read(s))
		# Upload that buffer
		self.num_part += 1
		# Go back to the beginning of the buffer again, so that we can continue writing there
		self.sio.reset()
		return sio_upload


	#def upload_buffer(self, sio_upload):
		#print sio_upload

	#def complete_mpu(self):
		#pass

	def run(self):
		self.create_new_buffer()
		current_period = None

		partitioner = zmq.Context().socket(zmq.PULL)
		partitioner.connect("ipc:///tmp/json2s3/%s" % (self.name))

		while True:

			# Get message
			message = partitioner.recv_pyobj()
			message_time = datetime.now()
			# Encode message as JSON
			try:
				json_data = dumps(message)
			except:
				self.log.exception("Skipping this message")
				continue

			if message_time.__getattribute__(self.granularity) != current_period:
				# We have to start a new multipart upload
				self.log.debug("We have entered a period")

				# But first, do I have to close any old multipart uploads?
				if self.num_messages > 0:
					# first upload remaining part
					self.log.debug("First upload remaining part")
					try:
						self.upload_buffer(last_part=True)
					except:
						self.start_new_mpu(previous_message_time)
						try:
							self.upload_buffer(self.get_buffer_to_upload(last_part=True))
						except:
							self.log.exception("Something went wrong with uploading the buffer")

					self.complete_mpu()
					self.create_new_buffer()

				current_period = message_time.__getattribute__(self.granularity)

			# Write JSON to buffer
			self.write_to_buffer(json_data)

			# Upload if buffer is full
			if self.sio.tell() > self.max_buffer_size:
				try:
					self.upload_buffer(self.get_buffer_to_upload())
				except:
					self.start_new_mpu(message_time)
					try:
						self.upload_buffer(self.get_buffer_to_upload())
					except:
						self.log.exception("Something went wrong with uploading the buffer")

			previous_message_time = message_time
