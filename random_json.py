#!/usr/bin/env python

import json
from sys import stdout
from random import randint, choice
from datetime import datetime
from time import sleep
import string

while True:
	message = {
		'time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
		'user': randint(1,3),
		#'value': "".join([choice(string.letters) for i in xrange(10000)])
	}

	stdout.write(json.dumps(message) + "\n")
	stdout.flush()
	sleep(1)
