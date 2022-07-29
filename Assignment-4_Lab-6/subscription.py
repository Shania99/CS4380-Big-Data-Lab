from google.cloud import pubsub_v1
from google.cloud import storage

sub = pubsub_v1.SubscriberClient()
topic_name = 'projects/graphite-byte-260703/topics/topic_lab6'
sub_name = 'projects/graphite-byte-260703/subscriptions/subscript'
sub = pubsub_v1.SubscriberClient()
sub.create_subscription(name=sub_name, topic=topic_name)

def callback(package):
    x = package
    print(x.data)
    with open('addresses.csv', 'r') as f: 
        count = 0
        for line in f:
            count = count+1
    print('Expected Line Count: ' + str(count)) 
    package.ack()

future = sub.subscribe(sub_name, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
