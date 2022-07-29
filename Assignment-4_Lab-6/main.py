
 def message(data, context):
    from google.cloud import pubsub_v1
    pub_client = pubsub_v1.PublisherClient()
    topic_name = 'projects/graphite-byte-260703/topics/topic_lab6'
    pub = pubsub_v1.PublisherClient()
    pub.create_topic(topic_name)
    out = data['name']
    out = out.encode("utf-8")
    pub_client.publish(topic_name, out)
    print("Message Received")

