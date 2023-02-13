import requests

try:
    r = requests.post('http://127.0.0.1:2135/api/get_info_wb/', json={
        "url": "https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation"})
    print(r.content.decode("utf-8"))
except Exception as e:
    print(e)
