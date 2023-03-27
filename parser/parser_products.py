from confluent_kafka import Producer, Consumer
import datetime
import yaml


def read_config():
    """Получение настроек из файла для дальнейшего применения в функциях"""
    with open('config.yaml') as f:
        # загрузка файла в виде словаря
        read_data = yaml.load(f, Loader=yaml.FullLoader)
    return read_data


def delivery_report(err, msg):
    """Вызывается один раз для каждого полученного сообщения, чтобы указать результат доставки.
    Запускается с помощью poll() или flush()."""
    # если ошибка есть
    if err is not None:  # ?????
        print('Ошибка доставки сообщения: {}'.format(err))
    else:
        # расшифровываем сообщение и приводим в формат словаря
        msg_value = eval(msg.value().decode('utf-8'))
        # вывводим дату в читаемом формате
        print('Сообщение {}, доставленно в {} [{}]'.format(
            {value if key != "time" else value.strftime("%Y-%m-%d %H:%M:%S") for key, value in msg_value.items()},
            msg.topic(), msg.partition()))


def save_answer_kafka(response):
    """Сохранение информации о каждом товаре страницы в топик **wb-products** в Kafka"""
    # получить настройки из файла
    config = read_config()
    # передача продюсеру названия сервера
    p = Producer({
        'bootstrap.servers': config["KAFKA_BROKER"]
    })

    # Добавление сообщения в очередь сообщений в топик (отправка брокеру)
    # callback - используется функцией pull или flush для последующего чтения данных отслеживания сообщения:
    # было ли успешно доставлено или нет
    p.produce(config["PRODUCER_DATA_TOPIC"], f'{response}', callback=delivery_report)

    # Дожидается доставки всех оставшихся сообщений и отчета о доставке
    # Если топик не создан, то он создается c 1 партицей по умолчанию (1 копия данных помещенных в топик)
    p.flush()


def get_data_from_topic():
    """Получить сырые данные из топика wb-category"""
    try:
        # получить настройки из файла
        config = read_config()
        # создать экземпляр потребителя
        # c = Consumer({
        #    'bootstrap.servers': config["KAFKA_BROKER"],
        #    'group.id': 'group_kafka'})
        # 'enable.auto.commit': False})
        # установить потребителю раздел, с которого надо читать данные
        # этот топик совпадает с тем топиком, куда отправил данные производитель
        # c.subscribe([config["PRODUCER_DATA_TOPIC"]])
        print("Запуск парсера. Потребитель создан и топик назначен")
        while True:
            c = Consumer({
                'bootstrap.servers': config["KAFKA_BROKER"],
                'group.id': 'group_kafka'})
            c.subscribe([config["PRODUCER_DATA_TOPIC"]])
            # запрашивает данные каждую миллисекунду. Если по истечению этого времени не получено ни одной записи,
            # то msg будет содержать пустой набор записей
            msg = c.poll(1.0)
            print("Я")
            # зафиксировать товары поступившие в обработку
            # c.commit()
            # если данные в топик еще не отправлены, то продолжаем ждать даннык
            if msg is None:
                continue
            # если получена ошибка при ????
            if msg.error():
                print("Ошибка при получении страницы с товрами из топика. {}".format(msg.error()))
                continue
            print("Беру")
            print(eval(msg.value().decode('utf-8'))['data']['products'][0])
            parse_products(msg.value(), datetime.datetime.now())
            # return 0
            c.close()
        print("Парсер закончил свою работу")
    except Exception as error:
        print(f"Ошибка парсера: {error}")


def parse_products(msg, time_of_receipt):
    """Парсинг товаров и отправка данных о каждом товаре в функцию сохранения товаров"""
    # расшифровка сообщения, взятие словаря данных о товарах
    products = eval(msg.decode('utf-8'))['data']['products']
    print("Получен список товаров из wb-category")
    # получение и отправка нужных данных о товаре из списка товаров
    for product in products:
        product = {"time": time_of_receipt, "id": product['id'], "name": product['name'],
                   "price": product['salePriceU'] / 100, "sale": product['sale']}
        # отправка данных о каждом товаре в топик
        save_answer_kafka(product)


if __name__ == '__main__':
    get_data_from_topic()
