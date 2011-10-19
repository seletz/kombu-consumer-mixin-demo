import logging

class WorkerBase(object):

    def info(self, msg, *args):
        self.logger.info("%s" % (msg % args))

    def error(self, msg, *args):
        self.logger.error("%s" % (msg % args))

    def debug(self, msg, *args):
        self.logger.debug("%s" % (msg % args))

def setup_logging(level):
    logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
