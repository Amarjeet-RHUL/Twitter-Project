from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

access_token = '1450393868478844936-viYKvz4eeqhGCGVoODkUoaG5pabONQ'
access_token_secret = 'dN7fB3edxThj6vLeoLIAM4dkBidzpdY1AhnA6FwYl6LUq'
consumer_key  ='EjfTwYtizNnavEyYA64T9Hw6q'
consumer_secret  ='zVcnudva8axbRvN9tK9IT7AsKtltVa25hgmyB5gqgbabVGpZDK'

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("twitter-data", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)
producer = KafkaProducer(bootstrap_servers="localhost:9092")
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["Russia", "Ukraine", "war", "peace", "NoWar"])
