from streamer import Streamer

from os import makedirs
from os.path import dirname

class LocalStreamer(Streamer):

	def start_new_mpu(self, now):
		partition_name = self.get_partition_name(now)
		self.log.info("Starting new MPU: %s", partition_name)

		if self.use_dir:
			try:
				makedirs(dirname(partition_name))
			except:
				pass

		try:
			self.mpu = open(partition_name, 'wb')
		except:
			self.log.exception("Could not start new MPU: %s", partition_name)
		self.num_part = 0


	def upload_buffer(self, sio_upload):
		self.log.info('Uploading buffer containing %d messages (part %d)', self.num_messages, self.num_part)
		self.mpu.write(sio_upload.getvalue())
		self.log.debug('Finished uploading buffer (part %d)', self.num_part)
		self.num_messages = 0


	def complete_mpu(self):
		try:
			self.mpu.close()
			self.log.debug("Completed MPU")
		except:
			self.log.debug("No MPU to complete")
		self.mpu = None
