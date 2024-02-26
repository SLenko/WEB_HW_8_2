import pika
from mongoengine import connect
from models import Contact
from faker import Faker

# Підключення до MongoDB
connect('contacts_db')

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()

channel.queue_declare(queue='email_queue')

# Генеруємо фейкові контакти та записуємо їх у базу даних
fake = Faker()

for _ in range(10):
    name = fake.name()
    email = fake.email()
    contact = Contact(name=name, email=email)
    contact.save()
    # Публікуємо повідомлення в чергу RabbitMQ з ObjectId контакту
    channel.basic_publish(exchange='', routing_key='email_queue', body=str(contact.id))
    print(f"Sent {name} to the email queue")

connection.close()
