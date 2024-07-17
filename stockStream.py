import time
import quandl
import argparse
import os
from time import strftime, gmtime
from kafka import KafkaProducer

NASDAQ = r"nasdaqlisted.txt"
QUANDL_EOD_API = r"https://www.quandl.com/api/v3/datasets/EOD/"
TOPIC = r"zhangfx_mpcs53014"
HOST = r"b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092"

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message '{}: {}' published successfully in topic {}.".format(key, value, topic_name))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    print("Start connecting!")
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=HOST)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    print("Connected!")

def test():
    kafka_producer = connect_kafka_producer()
    messages = 360
    sleepTime = 10
    while (messages > 0):
        messages = messages - 1
        showtime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        publish_message(kafka_producer, TOPIC, 'price', showtime)
        time.sleep(sleepTime)

if __name__ == '__main__':
    # add cli parse to accept token of quandl
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--num", type=int, default=0, help="how many to fetch")
    cli_parser.add_argument("--token", type=str, required=True, help="token for accessing quandl premium data")
    cli_args = cli_parser.parse_args()
    quandl_token = cli_args.token
    stock_num = cli_args.num
    print("Accept token ", quandl_token, "Num:", stock_num)

    # fetch file if not exists
    if not os.path.exists(NASDAQ):
        os.system("curl ftp://ftp.nasdaqtrader.com/symboldirectory/{} > {}".format(NASDAQ, NASDAQ))
        if not os.path.exists(NASDAQ):
            raise Exception("Failed to fetch file {}".format(NASDAQ))

    kafka_producer = connect_kafka_producer()
    stock_codes = []

    while(True):
        # read nasdaq list constantly
        with open(NASDAQ, 'r') as file:
            index = 0
            while True:
                index += 1
                stock_line = file.readline()
                try:
                    code = stock_line.split("|")[0]
                    # print(stock_line, code)
                    if index > 10:
                        break
                except:
                    print("ERROR: abnormal exit reading list!")
                    break

                if index > 1 and len(code)>0:
                    stock_codes.append(code)

                if (not stock_line and not stock_num) or (stock_num and index > stock_num):
                    break
                
            print("Finish Reading {} stocks ".format(index))
    
        # push yesterday stock data to kafka
        last_day = strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
        for stock in stock_codes:
            data = quandl.get("EOD/{}".format(stock), authtoken=quandl_token, start_date=last_day)
            publish_message(kafka_producer, TOPIC, stock, str(list(data["Adj_Low"].to_dict().values())[0]))
        
        index = quandl.get("NASDAQOMX/NDX", authtoken=quandl_token, start_date=last_day)
        publish_message(kafka_producer, TOPIC, "NDX", str(list(index["Index Value"].to_dict().values())[0]))

        # sleep
        time.sleep(86400)