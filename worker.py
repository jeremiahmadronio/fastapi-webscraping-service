"""
BudgetWise Scraper Microservice (Worker)
========================================

Description:
    This script acts as a background worker that listens for scraping tasks
    from the Main Spring Boot Application via RabbitMQ.

    It executes the scraping logic using Playwright/AsyncIO and returns
    the structured data back to the Java Backend.

Flow:
    1. Java sends a message to 'scrape_request_queue'.
    2. Python receives the URL.
    3. Python runs the scraper (main.py).
    4. Python constructs a JSON payload matching the Java DTO.
    5. Python sends the result to 'scraped_data_queue'.

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

# RabbitMQ Connection String (CloudAMQP)
# NOTE: In production, store this in an Environment Variable (.env) for security.
CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'

# Queue Definitions (Must match Java RabbitMQConfig)
REQUEST_QUEUE = 'scrape_request_queue'  # INPUT: Commands from Java
OUTPUT_QUEUE = 'scraped_data_queue'     # OUTPUT: Results to Java

def start_worker():
    """
    Initializes the RabbitMQ connection and starts the consumer loop.
    This function blocks and runs indefinitely until interrupted.
    """
    print(f" [*] Connecting to CloudAMQP...")

    try:
        # 1. Establish Connection
        params = pika.URLParameters(CLOUDAMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # 2. Declare Queues (Idempotent operation)
        # 'durable=True' ensures queues survive a broker restart.
        channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

        print(f" [*] Connected! Listening for tasks in '{REQUEST_QUEUE}'...")

        # ======================================================================
        # MESSAGE PROCESSOR (CALLBACK)
        # ======================================================================
        def callback(ch, method, properties, body):
            """
            Triggered whenever a message is received from the Request Queue.
            """
            print(f"\n [x] Command Received: {body.decode()}")

            try:
                # A. Parse the Incoming Request
                request_data = json.loads(body.decode())
                target_url = request_data.get('target_url')

                # Validation
                if not target_url:
                    print(" [!] Error: 'target_url' missing in request payload.")
                    ch.basic_ack(delivery_tag=method.delivery_tag) # Ack to remove invalid msg
                    return

                print(f" [x] Running scraper logic for: {target_url}...")

                # B. Execute the Scraper (Async Bridge)
                # Since pika is synchronous, we use asyncio.run to call the async scraper.
                result = asyncio.run(run_standalone_scraper(target_url))

                # C. Validate and Construct Payload
                if result and result.get('data'):
                    data = result['data']

                    # Extract necessary fields
                    price_data = data.get('price_data', [])
                    covered_markets = data.get('covered_markets', [])
                    # Use today's date if scraper didn't return one
                    date_processed = data.get('date_processed', str(date.today()))

                    item_count = len(price_data)

                    # ---------------------------------------------------------
                    # D. DATA TRANSFORMATION (CRITICAL STEP)
                    # We map the Python Dictionary to match the Java 'ScrapeResultDto' structure.
                    # ---------------------------------------------------------
                    payload = {
                        "status": "SUCCESS",
                        "date_processed": date_processed,
                        "original_url": target_url,
                        "covered_markets": covered_markets,
                        "price_data": price_data
                    }

                    print(f" [x] Success! Extracted {item_count} items from {len(covered_markets)} markets.")

                    # E. Publish Result back to Java
                    channel.basic_publish(
                        exchange='',
                        routing_key=OUTPUT_QUEUE,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent message (saved to disk)
                            content_type='application/json'
                        ))

                    print(f" [x] Data sent to '{OUTPUT_QUEUE}' successfully.")

                else:
                    print(" [!] Warning: Scraper returned empty data. Nothing sent to Java.")

            except Exception as e:
                # F. Error Handling
                print(f" [!] Error during processing: {e}")
                traceback.print_exc() # Print full stack trace for debugging

            # G. Acknowledge Message
            # Crucial! Tells RabbitMQ "I'm done, you can delete this message from the queue".
            # If we don't do this, the message will be stuck in "Unacked" state.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(" [x] Ready for next task...")

        # 3. Start Consuming
        # auto_ack=False means we manually send the ack (safer for long tasks)
        channel.basic_consume(queue=REQUEST_QUEUE, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()

    except Exception as e:
        print(f"CRITICAL SYSTEM ERROR: {e}")
        print("Please verify your CloudAMQP URL and Internet Connection.")

if __name__ == "__main__":
    try:
        start_worker()
    except KeyboardInterrupt:
        print('Worker stopped manually.')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)