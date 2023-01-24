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
    if subcategory.get('shardKey') is None:
        return None, None, None, None

    query = subcategory['query']
    query_split = query.split('&')
    kind = ''
    ext = ''
    subject = query_split[0]

    if query.find('ext') != - 1 and query.find('kind') != - 1:
        kind = '&' + query_split[0]
        subject = query_split[1]
        ext = '&' + query_split[2]

    elif query.find('ext') != - 1:
        ext = '&' + query_split[1]
        # subject = subcategory['query'].split('&')[0]

    elif query.find('kind') != - 1:
        kind = '&' + query_split[0]
        subject = query_split[1]

    return subcategory['shardKey'], kind, subject, ext


def find_the_right_subcategory(dict_var, page_url_category):
    for k, v in dict_var.items():
        if v == page_url_category:
            yield dict_var  # возвращает необходимую категорию
        elif isinstance(v, dict):  # если значение v является словарем
            for id_val in find_the_right_subcategory(v, page_url_category):
                yield id_val
        elif isinstance(v, list):  # если значение v является списком
            for dict_i in v:  # прохожу по каждому элементу списка
                for id_val in find_the_right_subcategory(dict_i, page_url_category):
                    yield id_val


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

    need_category = {}
    need_subcategory = {}
    # взять данные из подкатегории для последующего запроса о взятии товаров
    catalog = requests.get('https://catalog.wb.ru/menu/v6/api?lang=ru&locale=ru')
    # print(page_url_category)
    for category in catalog.json()['data']['catalog']:
        # найти нужную категорию товаров
        if category['pageUrl'] in page_url_category:
            need_category = category
    # найти нужную подкатегорию товаров
    for _ in find_the_right_subcategory(need_category, page_url_category[0]):
        need_subcategory = _
    print(need_subcategory)
    # достать необходимые для запроса данные
    shard_key, kind, subject, ext = get_category_data(need_subcategory)
    # сделать запрос и взять первые пять страниц
    get_pages(shard_key, kind, subject, ext, page_url_category)


# Получает информацию о товарах с первых 5 страниц категории через мобильное API Wildberries
def get_pages(shard_key, kind, subject, ext, page_url_category):
    response = requests.get("https://marketing-info.wildberries.ru/marketing-info/api/v6/info?curr=rub")
    client_params = {p.split('=')[0]: p.split('=')[1] for p in response.json()['xClientInfo'].split('&')}
    product_url = ""
    for page_number in range(1, 6):
        if page_url_category != '/promotions':
            # dest - это определение региона и центра выдачи товаров, склада (Это может быть направление
            # или область карты, параметры для выборки из бд, пока неясно, что это за координаты/границы)
            # spp - Так СПП - это скидка постоянного покупателя. Величина переменная, которая зависит от размера выкупа,
            # конкретного зарегистрированного покупателя. Но допустим, этот параметр вам нужен. В примере spp=26
            product_url = f"https://catalog.wb.ru/catalog/{shard_key}/catalog?" \
                          f"appType={client_params['appType']}&curr={client_params['curr']}" \
                          f"&dest={client_params['dest']}&emp={client_params['emp']}{ext}{kind}&" \
                          f"lang={client_params['lang']}&locale={client_params['locale']}&page={page_number}&" \
                          f"reg={client_params['reg']}&regions={client_params['regions']}&sort=popular&" \
                          f"spp={client_params['spp']}&{subject}&version={client_params['version']}"
        elif shard_key is None and subject is None:
            product_url = "https://www.wildberries.ru/promotions"
        response = requests.get(product_url).json()
        print(product_url)
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
    # get_api("https://www.wildberries.ru/promotions")
    # get_api("https://www.wildberries.ru/catalog/detyam/tovary-dlya-malysha/peredvizhenie/avtokresla-detskie")
    get_api("https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation")
