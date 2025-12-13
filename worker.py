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

# PASTE MO DITO YUNG URL GALING CLOUDAMQP (Nagsisimula sa amqps://)
CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'

# URL ng Spring Boot (Localhost muna tayo)
JAVA_API_URL = 'http://localhost:8080/api/ingestion/raw-data'
QUEUE_NAME = 'scrape_queue'

def start_worker():
    print(f" [*] Connecting to CloudAMQP...")

    try:
        params = pika.URLParameters(CLOUDAMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        print(f" [*] Connected! Waiting for messages in '{QUEUE_NAME}'...")

        def callback(ch, method, properties, body):
            print(f" [x] Command Received: {body.decode()}")

            try:
                # 1. Run Scraper
                print(" [x] Running scraper logic...")
                result = asyncio.run(run_standalone_scraper())

                if result:
                    item_count = len(result['data']['price_data'])
                    print(f" [x] SUCCESS! Scraped {item_count} items.")
                    print(f" [x] Sending to Java Backend ({JAVA_API_URL})...")

                    try:
                        # 2. Send to Java
                        response = requests.post(JAVA_API_URL, json=result)
                        if response.status_code == 200:
                            print(" [x] Java accepted the data! (200 OK)")
                        else:
                            print(f" [!] Java rejected: {response.status_code} - {response.text}")
                    except Exception as e:
                        print(f" [!] Failed to connect to Java: {e}")
                else:
                    print(" [!] Scraper returned empty/null.")

            except Exception as e:
                print(f" [!] Error executing scraper: {e}")

            print(" [x] Waiting for next command...")

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
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