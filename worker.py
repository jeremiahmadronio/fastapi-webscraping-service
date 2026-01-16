"""
BudgetWise Scraper Microservice (Worker)
============================================================
Description:
    Background worker that listens for scraping tasks AND manual uploads via RabbitMQ.
    Executes scraping logic and returns JSON-serializable data to Java.
"""

import pika
import json
import asyncio
import os
import sys
import traceback
import base64
from datetime import date

# Import the core scraping logic from main.py
# Ensure process_manual_pdf_bytes is defined in main.py
from main import run_standalone_scraper, process_manual_pdf_bytes

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# RabbitMQ Connection String
CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'

# Queue Definitions (Must match Java RabbitMQConfig)
REQUEST_QUEUE = 'scrape_request_queue'  # INPUT
OUTPUT_QUEUE = 'scraped_data_queue'     # OUTPUT

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
            print(f"\n [x] Command Received")

            try:
                # A. Parse Request
                try:
                    request_data = json.loads(body.decode())
                except json.JSONDecodeError:
                    print(" [!] Error: Invalid JSON Format")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                result = None

                # --- LOGIC BRANCHING ---

                # CASE 1: MANUAL UPLOAD (Check if file_content exists)
                if 'file_content' in request_data and request_data['file_content']:
                    print(" [x] Mode: MANUAL PDF UPLOAD detected.")

                    try:
                        # Decode Base64 string back to bytes
                        pdf_b64 = request_data['file_content']
                        filename = request_data.get('filename', 'manual_upload.pdf')

                        pdf_bytes = base64.b64decode(pdf_b64)

                        # Run logic directly (Synchronous)
                        result = process_manual_pdf_bytes(pdf_bytes, filename)
                    except Exception as e:
                        print(f" [!] Base64 Decode/Process Error: {e}")
                        traceback.print_exc()

                # CASE 2: WEB SCRAPING (Check if target_url exists)
                elif 'target_url' in request_data and request_data['target_url']:
                    target_url = request_data.get('target_url')
                    print(f" [x] Mode: WEB SCRAPING target: {target_url}")
                    # Run logic (Asynchronous)
                    result = asyncio.run(run_standalone_scraper(target_url))

                # CASE 3: INVALID PAYLOAD
                else:
                    print(" [!] Error: Invalid Payload. Missing 'target_url' or 'file_content'.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                # --- PROCESS RESULT ---

                # C. Validate & Convert Data
                if result and result.get('data'):
                    data = result['data']

                    # Fix Serialization (Pydantic to Dict)
                    raw_rows = data.get('price_data', [])

                    # Robust checking for Pydantic objects vs Dicts
                    price_data = []
                    for item in raw_rows:
                        if hasattr(item, 'model_dump'):
                            price_data.append(item.model_dump())
                        elif hasattr(item, 'dict'):
                            price_data.append(item.dict())
                        else:
                            price_data.append(item)

                    covered_markets = data.get('covered_markets', [])
                    date_processed = data.get('date_processed', str(date.today()))

                    item_count = len(price_data)

                    # D. Construct Payload
                    payload = {
                        "status": "SUCCESS",
                        "date_processed": date_processed,
                        "original_url": request_data.get('filename') if 'filename' in request_data else request_data.get('target_url'),
                        "covered_markets": covered_markets,
                        "price_data": price_data,
                        "source_type": "MANUAL" if 'file_content' in request_data else "SCRAPED"
                    }

                    print(f" [x] Success! Extracted {item_count} items from {len(covered_markets)} markets.")

                    # E. Publish Result
                    channel.basic_publish(
                        exchange='',
                        routing_key=OUTPUT_QUEUE,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type='application/json'
                        ))
                    print(f" [x] Data sent to '{OUTPUT_QUEUE}' successfully.")

                else:
                    print(" [!] Warning: Scraper/Parser returned empty data or failed.")

            except Exception as e:
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