import requests

print('start')
try:
    r = requests.post('http://127.0.0.1:5001/api/get_info_wb/', json={
        "url": "https://www.wildberries.ru/catalog/elektronika/razvlecheniya-i-gadzhety/igrovye-konsoli/playstation"})
    print(r)
    print(r.status_code)
    print(r.json())
except Exception as e:
    print(e)
