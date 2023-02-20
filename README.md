# Получение информации о товарах маркетплейса Wildberries из мобильного приложения

Для передачи ссылки необходимо обратиться по следующему адресу:
http://127.0.0.1:2135/api/get_info_wb/

Передать ссылку в формате jsom:
{"url": "https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation"}

Пример запроса приведен в файле query.py

Чтобы подключить network_mode в docker-compose.yaml необходимо узнать имя сети с помощью команды:
docker network ls

Чтобы узнать, какая сеть принадлежит kafka далее необходимо ввести команду:
docker network inspect {имя сети}

Нужная сеть будет иметь контейнеры kafka (ksqldb-server, zookeeper,...)
