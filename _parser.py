from confluent_kafka import Consumer
import json
import pprint


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
        save_product_information(msg.value())
        c.close()
        s = 1


def save_product_information(msg):
    msg = msg.decode('utf-8')
    products = eval(msg)['data']['products']
    for product in products:
        # print(product)
        print(product['id'])
        print(product['name'])
        print(product['salePriceU']/100)
        print(product['sale'])
    with open("test2.txt", "w", encoding="utf-8") as myfile:
        # pprint.pprint(msg.value())
        # f = json.dumps(msg, indent=2)
        myfile.write(str(products))
        # print(msg)


if __name__ == '__main__':
    get_data_from_topic()
