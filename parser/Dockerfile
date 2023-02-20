# установка базового образа
FROM python:3.9-slim-buster

# установка рабочей директории в контейнере
WORKDIR /parser

# Скачиваем/обновляем необходимые библиотеки для проекта 
COPY requirements.txt /parser
RUN pip3 install --upgrade pip -r requirements.txt

# |ВАЖНЫЙ МОМЕНТ| копируем содержимое папки, где находится Dockerfile, 
# в рабочую директорию контейнера
COPY . /parser
