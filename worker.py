"""
BudgetWise Scraper Microservice (Worker) - FIXED SERIALIZATION
============================================================

Description:
    Background worker that listens for scraping tasks via RabbitMQ.
    Executes scraping logic and returns JSON-serializable data to Java.

Flow:
    1. Listen to 'scrape_request_queue'.
    2. Run scraper (main.py).
    3. CONVERT Pydantic objects to Dictionaries (Critical Step).
    4. Send JSON result to 'scraped_data_queue'.

Author: Jeremiah (BudgetWise Team)
Date: Dec 2025
"""

import pika
import json
import asyncio
import os
import sys
import traceback
from datetime import date

# Import the core scraping logic
from main import run_standalone_scraper

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# RabbitMQ Connection String
CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'

# Queue Definitions (Must match Java RabbitMQConfig)
REQUEST_QUEUE = 'scrape_request_queue'  # INPUT: Commands from Java
OUTPUT_QUEUE = 'scraped_data_queue'     # OUTPUT: Results to Java

def start_worker():
    """
    Initializes the RabbitMQ connection and starts the consumer loop.
    """
    print(f" [*] Connecting to CloudAMQP...")

    try:
        # 1. Establish Connection
        params = pika.URLParameters(CLOUDAMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # 2. Declare Queues (Durable = True)
        channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

        print(f" [*] Connected! Listening for tasks in '{REQUEST_QUEUE}'...")

        # ======================================================================
        # MESSAGE PROCESSOR (CALLBACK)
        # ======================================================================
        def callback(ch, method, properties, body):
            print(f"\n [x] Command Received: {body.decode()}")

            try:
                # A. Parse Request
                request_data = json.loads(body.decode())
                target_url = request_data.get('target_url')

                if not target_url:
                    print(" [!] Error: 'target_url' missing.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                print(f" [x] Running scraper logic for: {target_url}...")

                # B. Execute Scraper
                result = asyncio.run(run_standalone_scraper(target_url))

                # C. Validate & Convert Data
                if result and result.get('data'):
                    data = result['data']

                    # ---------------------------------------------------------
                    # 🔥 CRITICAL FIX: CONVERT OBJECTS TO DICTIONARIES
                    # ---------------------------------------------------------
                    raw_rows = data.get('price_data', [])

                    # Convert each Pydantic 'PriceRow' object to a pure Dictionary
                    # This prevents "TypeError: Object of type PriceRow is not JSON serializable"
                    price_data = [item.model_dump() for item in raw_rows]

                    covered_markets = data.get('covered_markets', [])
                    date_processed = data.get('date_processed', str(date.today()))

                    item_count = len(price_data)

                    # D. Construct Payload
                    payload = {
                        "status": "SUCCESS",
                        "date_processed": date_processed,
                        "original_url": target_url,
                        "covered_markets": covered_markets,
                        "price_data": price_data  # Now pure JSON friendly
                    }

                    print(f" [x] Success! Extracted {item_count} items from {len(covered_markets)} markets.")

                    # E. Publish Result
                    channel.basic_publish(
                        exchange='',
                        routing_key=OUTPUT_QUEUE,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent message
                            content_type='application/json'
                        ))

                    print(f" [x] Data sent to '{OUTPUT_QUEUE}' successfully.")

                else:
                    print(" [!] Warning: Scraper returned empty data.")

            except Exception as e:
                # F. Error Handling
                print(f" [!] Error during processing: {e}")
                traceback.print_exc()

            # G. Acknowledge
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(" [x] Ready for next task...")

        # 3. Start Consuming
        channel.basic_consume(queue=REQUEST_QUEUE, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()

    except Exception as e:
        print(f"CRITICAL SYSTEM ERROR: {e}")
        print("Check CLOUDAMQP_URL or Internet Connection.")

if __name__ == "__main__":
    try:
        start_worker()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)