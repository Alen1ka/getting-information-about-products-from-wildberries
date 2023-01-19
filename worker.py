import requests
import json
from confluent_kafka import Producer


# Принимает на вход по API адрес категории на wildberries.ru
def read_conf():
    f = open('conf.json')
    data = json.load(f)
    server = data["default"]["bootstrap.servers"]
    topic_category = data["default"]["topic_category"]
    f.close()
    return server, topic_category


def get_category_data(subcategory):
    return subcategory['shardKey'], subcategory['query']


def get_api(url):
    page_url_category = []
    if url.find('https://www.wildberries.ru') != -1:
        # pageUrl подкатегории каталога
        # (например, /catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation)
        page_url_category.append(url[len(url) - url[::-1].index('https://www.wildberries.ru'[::-1]):])

    # изменить pageUrl подкатегории до pageUrl категории каталога
    count_find_slash = 0
    for i, c in enumerate(page_url_category[0]):
        if c == "/":
            count_find_slash += 1
            if count_find_slash >= 2:
                # pageUrl каждой подкатегории каталога после "/catalog/" (например, /catalog/elektronika)
                page_url_category.append(page_url_category[0][:i])

    subcategory_request_data = ''
    # взять данные из подкатегории для последующего запроса о взятии товаров
    catalog = requests.get('https://catalog.wb.ru/menu/v6/api?lang=ru&locale=ru')
    for category in catalog.json()['data']['catalog']:
        # найти нужную категорию товаров
        if category['pageUrl'] in page_url_category:
            # найти нужную подкатегорию товаров
            for subcategory in category.get(
                    'childNodes'):  # возможно прохожу не по всем подкатегориям или прохожу много раз
                # если подкатегория содержит подкатегории товаров
                while type(subcategory) is dict and subcategory.get('childNodes') is not None:
                    subcategory = subcategory.get('childNodes')
                    for subcategory_2 in subcategory:
                        # проверка на нужную подкатегорию товаров
                        if subcategory_2['pageUrl'] == page_url_category[0]:
                            subcategory_request_data = subcategory_2
                # если подкатегория не делится подкатегории товаров
                else:
                    # чтобы подкатегория которая использовалась в цикле while не выводилась полностью
                    if type(subcategory) is dict:
                        # проверка на нужную подкатегорию товаров
                        if subcategory['pageUrl'] == page_url_category[0]:
                            subcategory_request_data = subcategory
    # print(subcategory_request_data)
    shard_key, query = get_category_data(subcategory_request_data)
    get_pages(shard_key, query)  # electronic19, subject=523;524;526;527;532;593;844;982;1388;1407;3656;4072


# Получает информацию о товарах с первых 5 страниц категории через мобильное API Wildberries
def get_pages(shard_key, query):
    for page_number in range(1, 6):
        product = f'https://catalog.wb.ru/catalog/' \
                  f'{shard_key}' \
                  f'/catalog?appType=32&curr=rub&dest=-1029256,-102269,-2162196,-1257786&emp=0&' \
                  f'ext=91198;91199;164302;176241;177833;388495;396541&' \
                  f'lang=ru&locale=ru&page={page_number}' \
                  f'&reg=1&regions=1,4,22,30,31,33,38,40,48,64,66,68,69,70,71,75,80,83&sort=popular&' \
                  f'spp=30&' \
                  f'{query}&' \
                  f'version=3'
        response = requests.get(product).json()
        save_answer_kafka(response, page_number)


def delivery_report(err, msg):
    """ Вызывается один раз для каждого полученного сообщения, чтобы указать результат доставки.
    Запускается с помощью poll() или flush(). """
    if err is not None:
        print('Ошибка доставки сообщения: {}'.format(err))
    else:
        print('Сообщение, доставленно в {} [{}]'.format(msg.topic(), msg.partition(), msg.offcet()))


def send_msg_async(producer, topic, msg, page_number):
    print("Отправить сообщение асинхронно")
    producer.produce(
        topic,
        msg,
        callback=lambda err, original_msg=msg: delivery_report(err, original_msg),
        key=f'{page_number}'
    )
    producer.flush()


# Сохраняет каждый JSON ответ сервера отдельным сообщением в "сыром виде" в топик **wb-category** в Kafka
def save_answer_kafka(response, page_number):
    server, topic_category = read_conf()
    p = Producer({
        'bootstrap.servers': server
    })

    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce(topic_category, f'{response}', callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
    '''broker = f"localhost:9092"
    topic = "wb-category"

    producer = Producer({
        'bootstrap.servers': broker,
        # 'socket.timeout.ms': 100,
        # 'api.version.request': 'false',
        # 'broker.version.fallback': '0.9.0',
    })

    # отправка данных в топик Кафка
    send_msg_async(producer, topic,  f"{response}", page_number)'''


if __name__ == '__main__':
    get_api("https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation")
