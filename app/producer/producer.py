#from kafka import KafkaProducer
from utils import generate_review
import time

while True:
    print(generate_review(50))
    time.sleep(5)