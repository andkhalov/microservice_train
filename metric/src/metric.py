import pika
import json
import os
import pandas as pd

# Путь к файлу логов
LOG_FILE = './logs/metric_log.csv'

# Создаём пустой лог, если его нет
if not os.path.exists(LOG_FILE):
    df = pd.DataFrame(columns=['id', 'y_true', 'y_pred', 'absolute_error'])
    df.to_csv(LOG_FILE, index=False)

# Словари для временного хранения данных
received_y_true = {}
received_y_pred = {}

# Функция для записи в лог
def write_to_log(msg_id, y_true, y_pred):
    absolute_error = abs(y_true - y_pred)
    log_file_exists = os.path.exists(LOG_FILE)

    # Создаём DataFrame для строки
    new_row = pd.DataFrame([{
        'id': msg_id,
        'y_true': y_true,
        'y_pred': y_pred,
        'absolute_error': absolute_error
    }])

    # Добавляем заголовок только если файла ещё нет
    new_row.to_csv(LOG_FILE, mode='a', header=not log_file_exists, index=False)
    print(f"Записано в лог: id={msg_id}, y_true={y_true}, y_pred={y_pred}, absolute_error={absolute_error}")

def process_message(msg_id):
    if msg_id in received_y_true and msg_id in received_y_pred:
        y_true = received_y_true.pop(msg_id)
        y_pred = received_y_pred.pop(msg_id)
        write_to_log(msg_id, y_true, y_pred)

# Callback функции
def callback_y_true(ch, method, properties, body):
    message = json.loads(body)
    msg_id = message['id']
    received_y_true[msg_id] = message['body']
    process_message(msg_id)

def callback_y_pred(ch, method, properties, body):
    message = json.loads(body)
    msg_id = message['id']
    received_y_pred[msg_id] = message['body']
    process_message(msg_id)

# Настройка RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_pred')

channel.basic_consume(queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
channel.basic_consume(queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)

print("Ожидание сообщений. Нажмите CTRL+C для завершения.")
channel.start_consuming()