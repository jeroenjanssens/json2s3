#!/usr/bin/env python

import zmq
import logging
import argparse

from sys import stdin
from json import loads
from datetime import datetime


def get_partition_values_and_index(message, partition_keys):
	# TODO: Support nested fields
	if partition_keys:
		values = [message[k] for k in partition_keys]
		return values, tuple(values)
	else:
		return None, None

def get_ids():
	ids = {}
	cid = None
	with open(expanduser("~/.s3curl")) as f:
		for line in f.readlines():
			line = line.strip()
			if '=>' in line:
				k, v  = line.split('=>')[:2]
				k = k.strip()
				v = v.replace("'",'').replace('"','').replace(',','').strip()
				if v == '{':
					cid = k
					ids[cid] = {}
				else:
					ids[cid][k] = v
	return ids


def main():
	granularities = 'year,month,day,hour,minute,second,y,M,d,h,m,s'.split(',')
	log = logging.getLogger("json2s3")
	parser = argparse.ArgumentParser(description="Stream and partition JSON to AWS S3")
	parser.add_argument('-b', '--bucket', default=None, help="")
	parser.add_argument('-d', '--nodir', action="store_true", help="")
	parser.add_argument('-e', '--extension', default='.json.gz', help="")
	parser.add_argument('-f', '--nofile', action="store_true", help="")
	parser.add_argument('-g', '--granularity', default="hour", help="", choices=granularities)
	parser.add_argument('-i', '--id', default=None, help="")
	parser.add_argument('-k', '--keys', default=None, help="")
	parser.add_argument('-l', '--local', action="store_true", help="")
	parser.add_argument('-n', '--now', default=None, help="")
	parser.add_argument('-p', '--prefix', default='', help="")
	parser.add_argument('-v', '--verbose', action="store_true", help="")
	parser.add_argument('-V', '--veryverbose', action="store_true", help="")
	args = parser.parse_args()

	if args.verbose:
		log.setLevel(logging.INFO)
	if args.veryverbose:
		log.setLevel(logging.DEBUG)

	prefix = args.prefix
	extension = args.extension

	use_dir = not args.nodir
	use_file = not args.nofile

	if args.keys:
		partition_keys = args.keys.split(",")
	else:
		partition_keys = None

	gi = granularities.index(args.granularity)
	if gi > 5:
		gi -= 6
	granularity = granularities[gi]
	now_key = args.now

	get_new_streamer = None

	if not args.local:
		if not args.bucket:
			parser.error("please specify either --local or --bucket")
		if not args.id:
			parser.error("please specify the id")

		from s3_streamer import S3Streamer
		s3_id = get_ids()[args.id]


	else:
		from local_streamer import LocalStreamer
		log.debug("Local mode")

		def get_new_streamer(partition_values):
			return LocalStreamer(prefix, partition_keys, partition_values, granularity, extension, use_dir, use_file, logging.DEBUG)

	sockets = dict()
	streamers = dict()
	context = zmq.Context()

	do_skip_readline = False
	while True:
		try:
			if not do_skip_readline:
				line = stdin.readline()
				message = loads(line)
				pv, pi = get_partition_values_and_index(message, partition_keys)
				print message, partition_keys, pv, pi
			sockets[pi].send_pyobj(message)
			do_skip_readline = False
		except KeyError:
			print "new: ", pi
			streamers[pi] = get_new_streamer(pv)
			sockets[pi] = context.socket(zmq.PUSH)
			sockets[pi].bind("ipc:///tmp/json2s3/%s" % (streamers[pi].name))
			streamers[pi].start()
			do_skip_readline = True

if __name__ == '__main__':
	log_format = '%(asctime)-15s  [%(levelname)s] - %(name)s: %(message)s'
	logging.basicConfig(format=log_format, level=logging.ERROR)
	exit(main())
