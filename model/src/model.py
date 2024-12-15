import pika
import pickle
import numpy as np
import json

# Читаем файл с сериализованной моделью
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    # Создаём подключение по адресу rabbitmq
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue='features')
    channel.queue_declare(queue='y_pred')

    # Функция callback для обработки данных из очереди features
    def callback(ch, method, properties, body):
        try:
            # Десериализация сообщения
            message = json.loads(body)
            message_id = message['id']
            features = np.array(message['body']).reshape(1, -1)

            # Выполняем предсказание
            pred = regressor.predict(features)[0]

            # Формируем сообщение для очереди y_pred
            message_y_pred = {
                'id': message_id,
                'body': float(pred)  # Приводим к float для сериализации
            }

            # Отправляем предсказание в очередь y_pred
            channel.basic_publish(
                exchange='',
                routing_key='y_pred',
                body=json.dumps(message_y_pred)  # Сериализация в JSON
            )
            print(f"Предсказание отправлено в очередь: {message_y_pred}")
        except Exception as e:
            print(f"Ошибка обработки сообщения: {e}")

    # Настраиваем потребителя на очередь features
    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    # Запускаем режим ожидания
    channel.start_consuming()

except Exception as e:
    print(f"Не удалось подключиться к очереди: {e}")
