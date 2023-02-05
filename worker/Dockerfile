FROM python:3.9-slim-buster

RUN apt-get update
RUN pip install --upgrade pip
RUN pip install PyYAML
RUN pip install requests
RUN pip install Flask
RUN pip install confluent_kafka

# копирование всех файлов, включая зависимости, в рабочую директорию
COPY . /home/alena/python/getting-information-about-products-from-wildberries/worker
# установка рабочей директории в контейнере
WORKDIR /home/alena/python/getting-information-about-products-from-wildberries/worker

CMD ["python", "worker.py"]