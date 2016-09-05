import logging as log
from kombu import BrokerConnection
from kombu import Exchange
from kombu import Queue
from kombu.mixins import ConsumerMixin
import requests


EXCHANGE_NAME = "openstack"
ROUTING_KEY = "notifications.info"
QUEUE_NAME = "dump_queue"
BROKER_URI = "amqp://openstack:RABBIT_PASS@10.10.2.55:5672//"

ONOS_REST_URL = "http://10.10.106.97:8181/"
# ONOS_REST_URL = "http://10.10.2.47:8000/"


# log.basicConfig(stream=sys.stdout, level=log.DEBUG)
log.basicConfig(filename='/tmp/neutron_noti.log', level=log.DEBUG)


class NotificationsDump(ConsumerMixin):

    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, consumer, channel):
        exchange = Exchange(EXCHANGE_NAME, type="topic", durable=False)
        queue = Queue(QUEUE_NAME, exchange, routing_key=ROUTING_KEY, durable=False, auto_delete=True, no_ack=True)
        return [consumer(queue, callbacks=[self.neutron_message])]

    def neutron_message(self, body, message):
        print ('Body: %r' % body)
        log.info('Body: %r' % body)
        try:
            # r = requests.post(ONOS_REST_URL, data=body, verify=False, timeout=1)
            r = requests.post(ONOS_REST_URL, data=body, auth=('karaf','karaf'), verify=False, timeout=1)
            print ('Relay Result: %s' % r.status_code)
            # log.info('Relay Result: %s' % r.status_code)
        except Exception as e:
            print ('ONOS Send result: $r' % e)
        log.info('---------------')

if __name__ == "__main__":
    log.info('\n\n > Neutron Notification Relay tool start')
    log.info('%s' % '='*80)
    log.info("Connecting to broker {}".format(BROKER_URI))
    try:
        with BrokerConnection(BROKER_URI) as connection:
            print '> Neutron Notification Relay tool start'
            NotificationsDump(connection).run()
    except KeyboardInterrupt:
        print "bye"
