# -*- coding: utf-8 -*-
#
# File: slave.py
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

import logging

from kombu import BrokerConnection
from kombu import Queue
from kombu.utils import debug
from kombu.mixins import ConsumerMixin

from queues import announce_exchange
from queues import slave_queue_name
from queues import slave_routing_key

logger = logging.getLogger("slave")

class Slave(ConsumerMixin):
    def __init__(self, connection, job):
        logger.info("Slave.__init__: connection=%r" % connection)
        self.connection = connection
        self.job = job

    def get_consumers(self, Consumer, channel):
        queues = [
                Queue(
                    slave_queue_name(self.job),
                    announce_exchange,
                    routing_key=slave_routing_key(self.job),
                    auto_delete=True
                    )
                ]
        return [Consumer(queues=queues, callbacks=[self.process_message])]

    def process_message(self, body, message):
        logger.debug("Slave.process_message: message=%r" % message)
        logger.debug("Slave.process_message: body   =%r" % body)

        command = body.get("command", "print")
        logger.info("Slave: COMMAND %s" % command)

        if command == "print":
            print body

        if command == "quit":
            logger.info("Slave: QUITTING ...")
            self.should_stop = True

        message.ack()

def start_new_slave(job):
    logging.basicConfig(level=logging.INFO)
    debug.setup_logging(logging.INFO)
    logger.info("Slave: start_new_slave: job=%r" % job)
    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        Slave(conn, job).run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    #debug.setup_logging(logging.INFO)

# vim: set ft=python ts=4 sw=4 expandtab :
