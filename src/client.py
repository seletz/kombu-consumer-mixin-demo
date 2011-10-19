# -*- coding: utf-8 -*-
#
# File: client.py
#
# Copyright (c) nexiles GmbH
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

from __future__ import with_statement

__author__    = """Stefan Eletzhofer <se@nexiles.de>"""
__docformat__ = 'plaintext'

import sys
import time
import logging

from kombu import BrokerConnection
from kombu.utils import debug

from kombu.common import maybe_declare
from kombu.pools import producers

from queues import announce_exchange

logger = logging.getLogger("client")

def publish_job(job, routing_key="announce"):
    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        with producers[conn].acquire(block=True) as producer:
            maybe_declare(announce_exchange, producer.channel)
            if job.get("command", None) == "flood":
                job["command"] = "print"
                for x in range(100):
                    job["sequence"] = x
                    producer.publish(job, serializer="json", routing_key=routing_key)
                    logger.info("PUBLISH: %s route %s" % (job, routing_key))
            else:
                producer.publish(job, serializer="json", routing_key=routing_key)
                logger.info("PUBLISH: %s route %s" % (job, routing_key))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    debug.setup_logging(logging.DEBUG)

    if len(sys.argv) == 1:
        payload = {"message": "Hello World", "command": "new-slave", "job-id": str(time.time())}
        publish_job(payload)
        sys.exit(0)

    command = "print"
    if len(sys.argv) > 1:
        command = sys.argv[1]

    routing_key = "announce"
    if len(sys.argv) > 2:
        routing_key = sys.argv[2]

    arg = None
    if len(sys.argv) > 3:
        arg = sys.argv[3]

    job = dict(message="Hello World", command=command)
    if arg:
        job["argument"] = arg
    publish_job(job, routing_key)

# vim: set ft=python ts=4 sw=4 expandtab :
