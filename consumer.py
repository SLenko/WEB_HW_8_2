import pika
from mongoengine import connect
from models import Contact
from time import sleep

# Підключення до MongoDB
connect('contacts_db')

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()

channel.queue_declare(queue='email_queue')

def send_email(contact_id):
    # Імітація відправлення email
    # Тут можна використовувати реальний код для відправлення email
    print(f"Sent email to contact with ID: {contact_id}")
    # Оновлення логічного поля в базі даних
    contact = Contact.objects(id=contact_id).first()
    if contact:
        contact.email_sent = True
        contact.save()
        print(f"Updated email status for {contact.name}")

def callback(ch, method, properties, body):
    contact_id = body.decode()
    send_email(contact_id)

channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press Q')
channel.start_consuming()
