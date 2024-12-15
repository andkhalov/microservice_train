import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
from datetime import datetime
import time

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете
        X, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0] - 1)
        
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Создаём очереди y_true и features
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Генерируем уникальный идентификатор для текущего сообщения
        message_id = datetime.timestamp(datetime.now())

        # Формируем сообщение для очереди y_true
        message_y_true = {
            'id': message_id,
            'body': float(y[random_row])  # Преобразуем к float для сериализации
        }

        # Формируем сообщение для очереди features
        message_features = {
            'id': message_id,
            'body': list(X[random_row])  # Преобразуем к списку для сериализации
        }

        # Публикуем сообщение в очередь y_true
        channel.basic_publish(
            exchange='',
            routing_key='y_true',
            body=json.dumps(message_y_true)  # Сериализуем сообщение в JSON
        )
        print(f"Сообщение с правильным ответом отправлено в очередь: {message_y_true}")

        # Публикуем сообщение в очередь features
        channel.basic_publish(
            exchange='',
            routing_key='features',
            body=json.dumps(message_features)  # Сериализуем сообщение в JSON
        )
        print(f"Сообщение с вектором признаков отправлено в очередь: {message_features}")

        # Закрываем подключение
        connection.close()

        # Добавляем задержку между отправкой сообщений
        time.sleep(1)
    except Exception as e:
        print(f"Ошибка при подключении к очереди: {e}")
