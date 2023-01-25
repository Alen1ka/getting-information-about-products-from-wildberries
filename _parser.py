from confluent_kafka import Consumer
import json


# Принимает на вход по API адрес категории на wildberries.ru
def read_conf():
    f = open('conf.json')
    data = json.load(f)
    server = data["default"]["bootstrap.servers"]
    early_offset = data["consumer"]["auto.offset.reset"]
    topic_category = data["default"]["topic_category"]
    topic_products = data["consumer"]["topic_products"]
    f.close()
    return server, early_offset, topic_category, topic_products


def get_data_from_topic():
    """Получить сырые данные из топика wb-category"""
    server, early_offset, topic_category, topic_products = read_conf()
    c = Consumer({
        'bootstrap.servers': server,
        'group.id': 'mygroup',
        'auto.offset.reset': early_offset
    })

    c.subscribe([topic_category])
    s = 0
    while s == 0:
        msg = c.poll(1.0)  # запрашивает данные каждую миллисекунду

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: i')
        # print('Received message: {}'.format(msg.value()))
        with open("test.txt", "ab") as myfile:
            myfile.write(msg.value())
            c.close()
            s = 1


if __name__ == '__main__':
    get_data_from_topic()
