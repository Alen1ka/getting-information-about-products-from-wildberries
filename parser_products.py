from confluent_kafka import Consumer
import yaml
from worker import save_answer_kafka, read_config
import datetime


# Принимает на вход по API адрес категории на wildberries.ru
def get_data_from_topic():
    """Получить сырые данные из топика wb-category"""
    config = read_config()
    c = Consumer({
        'bootstrap.servers': config["KAFKA_BROKER"],
        'group.id': 'mygroup',
        'auto.offset.reset': config["AUTO_OFFSET_RESET"]
    })

    c.subscribe([config["PRODUCER_DATA_TOPIC"]])

    while True:
        msg = c.poll(1.0)  # запрашивает данные каждую миллисекунду

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: i')
        # print('Received message: {}'.format(msg.value()))
        time_of_receipt = datetime.datetime.now()
        save_product_information(msg.value(), time_of_receipt, config)
        c.close()


def save_product_information(msg, time_of_receipt, config):
    msg = msg.decode('utf-8')
    products = eval(msg)['data']['products']
    for product in products:
        save_answer_kafka(
            {"time": time_of_receipt, "id": product['id'], "name": product['name'],
             "price": product['salePriceU'] / 100, "sale": product['sale']}, config["CONSUMER_DATA_TOPIC"])


if __name__ == '__main__':
    get_data_from_topic()
