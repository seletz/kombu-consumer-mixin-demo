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
from kombu.common import maybe_declare
from kombu.pools import producers
from kombu.utils import debug
from kombu.mixins import ConsumerMixin

from queues import announce_queues
from queues import announce_exchange
from slave import start_new_slave

logger = logging.getLogger("master")

def publish_job(connection, job, routing_key):
    logger.info("PUBLISH: %r -> %s" % (job, routing_key))
    with producers[connection].acquire(block=True) as producer:
        maybe_declare(announce_exchange, producer.channel)
        producer.publish(job, serializer="json", routing_key=routing_key)

class Master(ConsumerMixin):

    def __init__(self, connection):
        logger.info("Master.__init__: connection=%r" % connection)
        self.connection = connection

        self.slaves = []
        self._slave_id = 0

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=announce_queues,
                        callbacks=[self.process_message])]

    def publish(self, job, routing_key):
        publish_job(self.connection, job, routing_key)

    def broadcast_slaves(self, message):
        logger.info("Master.broadcast_slaves: message=%r" % message)
        for slave in self.slaves:
            logger.info("Master.broadcast_slaves: slave: %r" % slave)
            routing_key = slave["routing_key"]
            self.publish(message, routing_key)

    def join_slaves(self):
        logger.info("Master.join_slaves")
        for slave in self.slaves:
            process = slave["process"]
            logger.info("Master.join_slaves: %r" % process)
            process.join()

    def process_message(self, body, message):
        logger.info("Master.process_message: message=%r" % message)
        logger.info("Master.process_message: body   =%r" % body)

        command = body.get("command", "print")

        if command == "print":
            print "MASTER: ", body

        if command == "new-slave":
            slave = self.start_slave(body)
            print "SLAVE #%(id)d routing_key: '%(routing_key)s'" % slave

        if command == "cancel":
            self.broadcast_slaves({"command": "quit"})
            logger.info("Master: QUITTING ...")
            self.broadcast_slaves({"command": "term"})
            self.join_slaves()
            self.should_stop = True

        if command == "bcast":
            self.broadcast_slaves(dict(message=body.get("message", ""), command="print"))

        message.ack()

    @property
    def slave_id(self):
        self._slave_id += 1
        return self._slave_id

    def start_slave(self, job):
        logging.info("Master.start_slave: about to start a new slave for job %r" % job)
        slave_id = self.slave_id
        queue_name  = "slave-%d" % slave_id
        routing_key = "slave-%d" % slave_id
        p = Process(target=start_new_slave, args=(job, queue_name, routing_key))
        p.start()
        logging.info("Master.start_slave: new slave started: %r" % p)
        slave = dict(id=slave_id, process=p, job=job, routing_key=routing_key, queue_name=queue_name)
        self.slaves.append(slave)
        return slave


def command_start():
    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        Master(conn).run()

def command_stop():
    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        publish_job(conn, dict(command="cancel"), "announce")

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
