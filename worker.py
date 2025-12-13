import pika
import json
import asyncio
import requests
import os
import sys


from main import run_standalone_scraper

# ==============================================================================
# CONFIGURATION
# ==============================================================================


CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'


REQUEST_QUEUE = 'scrape_request_queue'
OUTPUT_QUEUE = 'scraped_data_queue'

def start_worker():
    print(f" [*] Connecting to CloudAMQP...")

    try:
        params = pika.URLParameters(CLOUDAMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

        print(f" [*] Connected! Waiting for commands in '{REQUEST_QUEUE}'...")

        def callback(ch, method, properties, body):
            print(f" [x] Command Received: {body.decode()}")

            try:
                request_data = json.loads(body.decode())
                target_url = request_data.get('target_url')

                if not target_url:
                    print(" [!] Error: 'target_url' not found. Skipping.")
                    return

                print(f" [x] Running scraper for URL: {target_url}...")
                result = asyncio.run(run_standalone_scraper(target_url))

                if result and result.get('data', {}).get('price_data'):
                    price_data = result['data']['price_data']
                    item_count = len(price_data)
                    print(f" [x] SUCCESS! Scraped {item_count} items.")

                    channel.basic_publish(
                        exchange='',
                        routing_key=OUTPUT_QUEUE,
                        body=json.dumps(price_data), # Send the JSON list
                        properties=pika.BasicProperties(
                            delivery_mode=2 # Gawing persistent ang message
                        ))

                    print(f" [x] Sent {item_count} items to '{OUTPUT_QUEUE}' queue.")

                else:
                    print(" [!] Scraper returned empty/null data.")

            except Exception as e:
                print(f" [!] Error executing scraper or sending data: {e}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(" [x] Waiting for next command...")

        channel.basic_consume(queue=REQUEST_QUEUE, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        print("Check your CLOUDAMQP_URL if correct.")

if __name__ == "__main__":
    try:
        start_worker()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)