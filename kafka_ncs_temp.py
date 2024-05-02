import pickle

from kafka import KafkaProducer
import datetime
from dotenv import load_dotenv
import os

load_dotenv()
kafka_address = os.getenv("KAFKA_address")
topic_raw = os.getenv("KAFKA_topic")
topic_update = os.getenv("KAFKA_topic_udpate")

producer = KafkaProducer(bootstrap_servers=["172.168.200.202:9092"])



def push_kafka(posts):
    if posts:
        bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
        producer.send('osint-posts-raw', bytes_obj)
        print(f"{datetime.datetime.now()} »» Đẩy 1 object vào kafka")
        return 1
    else:
        return 0
# def push_kafka_update(posts, comments):
#     if posts:
#         bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
#         producer.send('osint-posts-update', bytes_obj)
#         print(f"{Colorlog.cyan_color}{datetime.datetime.now()} »» Đẩy 1 object update  vào kafka {Colorlog.reset_color}")
#         return 1
#     else:
#         return 0


class GeneratorPost:
    def __init__(self, target, args: list = []) -> None:
        self.target = target
        self.args = args

    def run(self):
        for posts in self.target(*self.args):
            print(f"số bài post group đẩy qua kafka là {len(posts)}")
            push_kafka(posts=posts)

    def get_posts(self, list_posts: list):
        for posts in self.target(*self.args):
            print(f"số bài posts là {len(posts)}")
            list_posts.extend(posts)
            push_kafka(posts=posts)
            # for post in posts:
            #     write_log_post(post)
