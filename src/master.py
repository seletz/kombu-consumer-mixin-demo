# -*- coding: utf-8 -*-
#
# File: master.py
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
import logging

from multiprocessing import Process

from kombu import BrokerConnection
from kombu.utils import debug
from kombu.mixins import ConsumerMixin

from queues import announce_queues
from queues import slave_routing_key
from client import publish_job
from slave import start_new_slave

logger = logging.getLogger("master")

class Master(ConsumerMixin):

    def __init__(self, connection):
        logger.info("Master.__init__: connection=%r" % connection)
        self.connection = connection

        self.slaves = []

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=announce_queues,
                        callbacks=[self.process_message])]

    def broadcast_slaves(self, message):
        logger.info("Master.broadcast_slaves: message=%r" % message)
        for job, process in self.slaves:
            routing_key = slave_routing_key(job)
            publish_job(message, routing_key)

    def join_slaves(self):
        logger.info("Master.join_slaves")
        for job, process in self.slaves:
            logger.info("Master.join_slaves: %r" % process)
            process.join()

    def process_message(self, body, message):
        logger.debug("Master.process_message: message=%r" % message)
        logger.debug("Master.process_message: body   =%r" % body)

        if "job-id" in body.keys():
            job, p = self.start_slave(body)
            print "SLAVE #%d routing_key: '%s'" % (len(self.slaves), slave_routing_key(job))

        if "cancel" in body.keys():
            self.broadcast_slaves({"command": "quit"})
            logger.info("Master: QUITTING ...")
            self.broadcast_slaves({"command": "term"})
            self.join_slaves()
            self.should_stop = True

        message.ack()

    def start_slave(self, job):
        logging.info("Master.start_slave: about to start a new slave for job %r" % job)
        p = Process(target=start_new_slave, args=(job,))
        p.start()
        logging.info("Master.start_slave: new slave started: %r" % p)
        self.slaves.append((job,p))
        return job, p


def command_start():
    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        Master(conn).run()

def command_stop():
    payload = {"cancel": True}
    publish_job(payload)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    debug.setup_logging(logging.INFO)

    command = "start"
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

    if command == "start":
        command_start()
    
    if command == "stop":
        command_stop()

    sys.exit(0)

# vim: set ft=python ts=4 sw=4 expandtab :
