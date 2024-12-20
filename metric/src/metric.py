import pika
import json
import csv
import os

# Убедимся, что файл metric_log.csv существует и содержит заголовки
log_file = './logs/metric_log.csv'
os.makedirs('./logs', exist_ok=True)  # Создаём папку, если её нет

# Проверяем, существует ли файл, если нет — создаём с заголовками
if not os.path.exists(log_file) or os.stat(log_file).st_size == 0:
    with open(log_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'y_true', 'y_pred', 'absolute_error'])  # Заголовки

# Инициализируем структуру для хранения сообщений
messages = {}

try:
    # Создаём подключение к серверу RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_pred')

    # Функция для записи данных в CSV
    def log_to_csv(message_id, y_true, y_pred):
        try:
            absolute_error = abs(y_true - y_pred)
            with open(log_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([message_id, y_true, y_pred, absolute_error])
            print(f'Записано в лог: id={message_id}, y_true={y_true}, y_pred={y_pred}, error={absolute_error}')
        except Exception as e:
            print(f'Ошибка записи в CSV: {e}')

    # Callback для обработки сообщений из очередей
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            message_id = data.get('id')
            
            if method.routing_key == 'y_true':
                value = data.get('body')  # Берем значение из ключа 'body' для y_true
                if message_id not in messages:
                    messages[message_id] = {}
                messages[message_id]['y_true'] = value
            elif method.routing_key == 'y_pred':
                value = data.get('y_pred')  # Берем значение из ключа 'y_pred' для y_pred
                if message_id not in messages:
                    messages[message_id] = {}
                messages[message_id]['y_pred'] = value

            # Если у нас есть и y_true, и y_pred, логируем данные
            if 'y_true' in messages[message_id] and 'y_pred' in messages[message_id]:
                log_to_csv(
                    message_id,
                    messages[message_id]['y_true'],
                    messages[message_id]['y_pred']
                )
                # Удаляем запись после логирования
                del messages[message_id]

        except Exception as e:
            print(f'Ошибка обработки сообщения: {e}')

    # Подписываемся на очереди
    channel.basic_consume(queue='y_true', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='y_pred', on_message_callback=callback, auto_ack=True)

    # Запускаем режим ожидания сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

except Exception as e:
    print(f'Ошибка подключения или обработки: {e}')

finally:
    # Закрываем соединение при завершении
    if 'connection' in locals() and connection.is_open:
        connection.close()