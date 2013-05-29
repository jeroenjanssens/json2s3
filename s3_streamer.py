from streamer import Streamer

from boto.s3.key import Key
from boto.s3.connection import S3Connection

class S3Streamer(Streamer):

	def start_new_mpu(self, now):
		partition_name = self.get_partition_name(now)
		self.log.info("Starting new MPU: %s", partition_name)
		try:
			self.mpu = self.bucket.initiate_multipart_upload(partition_name)
		except:
			self.log.exception("Could not start new MPU: %s", partition_name)
		self.num_part = 0


	def upload_buffer(self, sio_upload):
		self.log.info('Uploading buffer containing %d messages (part %d)', self.num_messages, self.num_part)
		self.mpu.upload_part_from_file(sio_upload, self.num_part, cb=self.upload_callback)
		self.log.debug('Finished uploading buffer (part %d)', self.num_part)
		self.num_messages = 0


	def upload_callback(self, current, total):
		self.log.debug("Upload progress: %7s / %7s", current, total)


	def complete_mpu(self):
		try:
			self.mpu.complete_upload()
			self.log.debug("Completed MPU")
		except:
			self.log.debug("No MPU to complete")
		self.mpu = None
