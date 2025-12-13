# 🌾 DA Price Index Scraper & Processor

A FastAPI-based microservice that automates the extraction and processing of Daily Price Index (DPI) PDFs from the Department of Agriculture Philippines website. Features CloudAMQP (RabbitMQ) integration for asynchronous job processing, intelligent multi-line buffering, and brand-aware commodity normalization.

## 🚀 Live Deployment

- **Production URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app`
- **API Documentation:** [/docs](https://fastapi-webscrapping-pdfextractor-production.up.railway.app/docs)
- **Alternative Docs:** [/redoc](https://fastapi-webscrapping-pdfextractor-production.up.railway.app/redoc)

---

## ✨ Core Features

### 📬 CloudAMQP Integration
- **Asynchronous Processing** - Jobs queued via RabbitMQ for non-blocking operations
- **Worker-based Architecture** - Separate worker process handles PDF extraction
- **Reliable Message Delivery** - Guaranteed job processing with CloudAMQP
- **Direct Java API Integration** - Sends parsed data directly to Spring Boot backend

### 🔍 Intelligent PDF Processing
- **Multi-line Buffering** - Handles commodity names that span multiple lines in PDFs
- **Smart Text Extraction** - Uses pypdf for reliable PDF content extraction
- **Date Auto-detection** - Automatically extracts dates from PDF filenames
- **Market Identification** - Extracts covered markets from PDF content
- **Advanced Header Filtering** - Aggressive removal of header/footer noise

### 🏷️ Data Normalization & Categorization
- **Brand-aware Processing** - Distinguishes between branded and generic products
- **Category Mapping** - Automatically categorizes items (Rice, Vegetables, Meat, etc.)
- **Unit Standardization** - Normalizes units of measurement (kg, pc, L, ml)
- **Price Validation** - Validates and formats price data
- **Origin Detection** - Identifies Local vs Imported products

### ⚡ Performance
- **Async Operations** - Non-blocking HTTP requests with httpx
- **Queue-based Processing** - Background workers handle heavy PDF operations
- **In-memory Processing** - No file storage, processes PDFs in memory
- **Fast Response Times** - Immediate job acceptance, async processing

---

## 📋 API Endpoints

### 1. Health Check
Check if the service is running.

```http
GET /
```

**Response:**
```json
{
  "message": "Smart DA Price Scraper is Running (RabbitMQ Ready)"
}
```

---

### 2. Scrape Latest PDF (Direct Processing)
Directly scrapes and returns the newest Daily Price Index PDF data from DA website.

```http
POST /api/scrape-new-pdf
```

**Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "target_url": "https://www.da.gov.ph/price-monitoring/"
}
```

**Response:**
```json
{
  "status": "Success",
  "date_processed": "2024-12-10",
  "original_url": "https://www.da.gov.ph/uploads/...",
  "covered_markets": [
    "Balintawak Market",
    "Farmers Market Cubao",
    "Kamuning Market"
  ],
  "price_data": [
    {
      "category": "COMMERCIAL RICE",
      "commodity": "Well Milled Rice",
      "origin": "Local",
      "unit": "kg",
      "price": 52.50
    }
  ]
}
```

**Note:** This endpoint processes synchronously and returns immediately with results. For background processing, use the RabbitMQ worker.

---

### 3. Manual PDF Upload (Direct Processing)
Upload and parse a PDF file directly.

```http
POST /api/extract-manual
```

**Body:** `multipart/form-data`
- **Key:** `file`
- **Value:** PDF file (DPI format)

**Response:**
```json
{
  "status": "Success (Manual)",
  "date_processed": "2024-12-10",
  "original_url": "Manual: your-filename.pdf",
  "covered_markets": [...],
  "price_data": [...]
}
```

---

## 🐰 RabbitMQ Worker Architecture

### How It Works

```
RabbitMQ Queue → Worker Process → Scraper Logic → Java Spring Boot API
```

### Components

**main.py** - FastAPI application
- Provides HTTP endpoints for direct access
- Contains `run_standalone_scraper()` function for worker usage
- Handles PDF extraction and parsing logic

**worker.py** - Background worker
- Connects to CloudAMQP
- Listens to `scrape_queue` for commands
- Executes `run_standalone_scraper()` when message received
- Sends parsed data to Java backend at `http://localhost:8080/api/ingestion/raw-data`

### Worker Configuration

```python
# CloudAMQP Connection
CLOUDAMQP_URL = 'amqps://acyxmzrb:tPkTAhWMQ0ju6dxyQ6f0xdiijpfPspKd@codfish.rmq.cloudamqp.com/acyxmzrb'

# Queue Name
QUEUE_NAME = 'scrape_queue'

# Java API Endpoint
JAVA_API_URL = 'http://localhost:8080/api/ingestion/raw-data'
```

### Message Flow

1. **Message arrives** in RabbitMQ queue
2. **Worker receives** command and starts scraping
3. **Scraper downloads** latest PDF from DA website
4. **Parser extracts** and normalizes price data
5. **Worker sends** structured data to Java API
6. **Java backend** receives and processes data
7. **Worker waits** for next command

---

## 🛠️ Local Development

### Prerequisites
- Python 3.10 or higher
- pip (Python package manager)
- CloudAMQP account (or local RabbitMQ instance)
- Virtual environment (recommended)

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/jeremiahmadronio/fastapi-webscraping-service.git
   cd fastapi-webscraping-service
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   ```

3. **Activate virtual environment**
   
   **Windows (PowerShell):**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
   
   **Windows (Command Prompt):**
   ```cmd
   .venv\Scripts\activate.bat
   ```
   
   **macOS/Linux:**
   ```bash
   source .venv/bin/activate
   ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure worker.py**
   
   Edit `worker.py` and set your URLs:
   ```python
   CLOUDAMQP_URL = 'amqps://your-cloudamqp-url'
   JAVA_API_URL = 'http://localhost:8080/api/ingestion/raw-data'
   ```

6. **Run the application**
   
   **Terminal 1 - FastAPI Server (Optional):**
   ```bash
   uvicorn main:app --reload
   ```
   
   **Terminal 2 - Worker Process:**
   ```bash
   python worker.py
   ```

   The server will start at `http://localhost:8000`
   - **Interactive API Docs:** http://localhost:8000/docs
   - **Alternative Docs:** http://localhost:8000/redoc

---

## 📦 Project Structure

```
fastapi-webscraping-service/
├── main.py                 # FastAPI app + scraper logic + parser
│                          # Contains run_standalone_scraper()
├── worker.py              # RabbitMQ consumer + Java API sender
├── requirements.txt       # Python dependencies
├── Procfile              # Railway deployment config
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

---

## 📚 Dependencies

### Core Framework
- **FastAPI** - Modern, fast web framework for building APIs
- **Uvicorn** - Lightning-fast ASGI server
- **Pydantic** - Data validation using Python type hints

### Message Queue
- **pika** - Python RabbitMQ client for CloudAMQP integration

### Web Scraping & Parsing
- **httpx** - Async HTTP client for web requests
- **BeautifulSoup4** - HTML parsing and web scraping
- **lxml** - XML/HTML parser backend
- **pypdf** - PDF text extraction and processing

### Utilities
- **python-multipart** - File upload support for FastAPI
- **requests** - HTTP library for Java API calls

See `requirements.txt` for complete list with versions.

---

## 📊 Supported Categories

The scraper recognizes and normalizes the following DA price categories:

### Grains & Staples
- **Commercial Rice (Imported/Local)**
  - Basmati, Glutinous, Jasponica
  - Premium (5% broken)
  - Well Milled (1-19% bran streak)
  - Regular Milled (20-40% bran streak)
- **Corn Products**
  - White/Yellow Cob
  - Grits (Food/Feed Grade)
  - Cracked Corn

### Protein Sources
- **Fish Products**
  - Alumahan, Bangus, Bonito, Galunggong
  - Mackerel, Pampano, Salmon
  - Sardines, Squid, Tambakol, Tilapia
- **Meat Products**
  - **Beef:** Tenderloin, Sirloin, Ribs, etc.
  - **Pork:** Belly (Liempo), Shoulder (Kasim)
  - **Poultry:** Chicken (Magnolia/Bounty Fresh/Unbranded), Eggs

### Produce
- **Lowland Vegetables**
  - Cabbage varieties, Eggplant, Tomato
  - Squash, Bitter Gourd, String Beans
- **Highland Vegetables**
  - Bell Pepper, Broccoli, Cauliflower
  - Carrots, Lettuce varieties, Potato
  - Pechay, Baguio Beans
- **Spices**
  - Chilli (Red/Green), Garlic, Ginger
  - Onion (Red/White)
- **Fruits**
  - Banana varieties (Lakatan, Latundan, Saba)
  - Mango (Carabao), Papaya

### Other Commodities
- **Cooking Oil** (Palm, Coconut, Branded)
- **Sugar** (Refined, Washed, Brown)
- **Salt** (Iodized, Rock)

---

## 🧪 Testing the API

### Using cURL

**Scrape Latest PDF:**
```bash
curl -X POST "https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/scrape-new-pdf" \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://www.da.gov.ph/price-monitoring/"}'
```

**Manual Upload:**
```bash
curl -X POST "https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/extract-manual" \
  -F "file=@/path/to/your/dpi.pdf"
```

### Using Postman

#### Scrape Latest PDF
1. **Method:** `POST`
2. **URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/scrape-new-pdf`
3. **Headers:**
   - `Content-Type`: `application/json`
4. **Body (raw JSON):**
   ```json
   {
     "target_url": "https://www.da.gov.ph/price-monitoring/"
   }
   ```
5. Click **Send**

#### Manual Upload
1. **Method:** `POST`
2. **URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/extract-manual`
3. **Body:** Select `form-data`
   - Key: `file` (Type: File)
   - Value: Select your PDF file
4. Click **Send**

### Testing RabbitMQ Worker

**Send test message to queue:**
```python
import pika

connection = pika.BlockingConnection(
    pika.URLParameters('amqps://your-cloudamqp-url')
)
channel = connection.channel()
channel.queue_declare(queue='scrape_queue', durable=True)
channel.basic_publish(
    exchange='',
    routing_key='scrape_queue',
    body='START_SCRAPE'
)
print("Message sent to queue!")
connection.close()
```

---

## 🚢 Deployment

### Current Deployment: Railway

The application is deployed on [Railway](https://railway.app) with:
- **Automatic deployments** from `main` branch
- **CloudAMQP integration** for message queue
- **Production URL** with HTTPS

### Deployment Configuration

The `Procfile` defines both web and worker processes:
```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
worker: python worker.py
```

### Environment Variables
Required environment variables:
- `PORT` - Automatically set by Railway
- `CLOUDAMQP_URL` - Set in worker.py (hardcoded or use env var)
- `JAVA_API_URL` - Set in worker.py

### Setting up CloudAMQP

1. **Create account** at [cloudamqp.com](https://www.cloudamqp.com/)
2. **Create instance** (free tier: Little Lemur)
3. **Copy AMQP URL** from instance details
4. **Update worker.py** with your URL
5. **Deploy to Railway**

### Manual Deployment to Other Platforms

**Requirements:**
1. Platform with Python 3.10+ support
2. CloudAMQP or RabbitMQ instance
3. Support for background workers
4. Java Spring Boot backend accessible

**Deployment Steps:**
1. Push code to your platform
2. Configure `CLOUDAMQP_URL` in worker.py
3. Configure `JAVA_API_URL` in worker.py
4. Set `PORT` environment variable (if required)
5. Deploy both web and worker processes

---

## ⚙️ Configuration

### Scraping Configuration
Key configurations in `main.py`:

```python
BASE_URL = "https://www.da.gov.ph"
TARGET_URL = "https://www.da.gov.ph/price-monitoring/"
```

### Worker Configuration
RabbitMQ and API settings in `worker.py`:

```python
# CloudAMQP Connection String
CLOUDAMQP_URL = 'amqps://username:password@host/vhost'

# Queue Name
QUEUE_NAME = 'scrape_queue'

# Java Backend API
JAVA_API_URL = 'http://localhost:8080/api/ingestion/raw-data'
```

### Parser Settings
The parser in `main.py` includes:
- Multi-line commodity name detection with lookahead
- Smart unit extraction from specification column
- Robust brand and noise word removal
- Parenthetical description handling
- Improved price detection to avoid false matches
- Aggressive header/footer filtering

---

## 🐛 Troubleshooting

### Common Issues

#### "Connection to RabbitMQ failed"
- **Cause:** Invalid CLOUDAMQP_URL or network issue
- **Check:** Verify URL format: `amqps://user:pass@host/vhost`
- **Solution:** Test connection in CloudAMQP dashboard

#### "Worker not processing messages"
- **Cause:** Worker process not running or crashed
- **Check:** Look for error messages in worker logs
- **Solution:** Restart worker: `python worker.py`

#### "Failed to connect to Java"
- **Cause:** Java backend not running or wrong URL
- **Check:** Verify `JAVA_API_URL` is correct
- **Solution:** Start Spring Boot backend first

#### "Java rejected: 400/500 status"
- **Cause:** Data format mismatch with Java API
- **Check:** Java API expects specific JSON structure
- **Solution:** Review Java API contract and adjust worker payload

#### "No Daily Price Index PDFs found"
- **Cause:** DA website structure changed
- **Check:** PDF link patterns in `main.py`
- **Solution:** Update regex pattern in `scrape_new_pdf_data()`

#### "Invalid PDF format"
- **Cause:** Uploaded file is not a valid DPI PDF
- **Check:** File is from DA official source
- **Solution:** Use official DA price monitoring PDFs only

#### "Worker keeps retrying same job"
- **Cause:** Job fails but message not acknowledged
- **Check:** Worker logs for exception details
- **Solution:** Fix error in scraper logic, restart worker

---

## 🔒 Security Considerations

- **No API Keys** - Endpoints are public (use for internal systems only)
- **HTTPS:** All production requests use encrypted connections
- **Input Validation:** All inputs validated with Pydantic models
- **No Data Storage:** PDFs processed in-memory and not stored
- **CloudAMQP Security:** Uses secure AMQPS protocol
- **Network Security:** Java API should be behind firewall/VPN

---

## 📝 Development Notes

### Architecture
- **Decoupled Design:** FastAPI and worker are independent
- **Queue-based:** RabbitMQ handles async communication
- **Direct Integration:** Worker sends directly to Java API

### Parser Logic (v7.0.0)
- **Rule-based extraction** optimized for DA PDF format
- **Multi-line buffering** with lookahead detection
- **Brand-aware normalization** prevents data mixing
- **Advanced header filtering** removes pagination/headers
- **Specification extraction** from multi-line cells
- **Origin detection** from category and line content

### Worker Process
- **Blocking connection** to RabbitMQ (pika.BlockingConnection)
- **Auto-reconnect** on connection drop
- **Synchronous processing** - one job at a time
- **Error handling** with detailed logging
- **HTTP POST** to Java API with JSON payload

### Data Flow to Java

**Payload structure:**
```json
{
  "date": "2024-12-10",
  "data": {
    "covered_markets": [...],
    "price_data": [...]
  }
}
```

---

## 🔄 Complete System Flow

```
1. CloudAMQP Queue → Message arrives ("START_SCRAPE")
2. Worker Process → Detects message
3. run_standalone_scraper() → Fetches DA website
4. Scraper → Downloads latest PDF
5. Parser → Extracts commodity data
6. Normalizer → Cleans and categorizes
7. Worker → Sends to Java API (POST request)
8. Java Backend → Receives and stores data
9. Worker → Logs success and waits for next message
```

---

## 📌 Integration Guide

### For Java Backend Developers

**Expected Endpoint:**
- **URL:** `http://localhost:8080/api/ingestion/raw-data`
- **Method:** POST
- **Content-Type:** application/json

**Expected Payload:**
```json
{
  "date": "2024-12-10",
  "data": {
    "covered_markets": ["Market 1", "Market 2"],
    "price_data": [
      {
        "category": "COMMERCIAL RICE",
        "commodity": "Well Milled Rice",
        "origin": "Local",
        "unit": "kg",
        "price": 52.50
      }
    ]
  }
}
```

**Expected Response:**
- **Success:** 200 OK
- **Error:** 4xx/5xx with error message

### Sending Commands to Worker

**Using Python:**
```python
import pika

connection = pika.BlockingConnection(
    pika.URLParameters('amqps://your-cloudamqp-url')
)
channel = connection.channel()
channel.basic_publish(
    exchange='',
    routing_key='scrape_queue',
    body='START_SCRAPE'
)
connection.close()
```

**Using RabbitMQ Management Console:**
1. Login to CloudAMQP dashboard
2. Go to RabbitMQ Manager
3. Navigate to Queues
4. Select `scrape_queue`
5. Publish message manually

---

## 👥 Contributing

This is a private project for the Price Monitoring & Budget Planner System.

---

## 📄 License

**Private Project - All Rights Reserved**

This project is proprietary and confidential. Unauthorized copying, distribution, or use is strictly prohibited.

---

## 👤 Author

**Jeremiah Madronio**

---

## 📞 Support & Contact

For technical support or questions:
- Check the API documentation at `/docs`
- Review worker logs for error messages
- Check RabbitMQ queue status in CloudAMQP dashboard
- Verify Java backend is running and accessible

---

## 🙏 Acknowledgments

- **Department of Agriculture Philippines** - Data source
- **FastAPI** - Web framework
- **CloudAMQP** - Managed RabbitMQ service
- **Railway** - Deployment platform
- **Pika** - Python RabbitMQ client

---

## 📌 Quick Links

- [Production API](https://fastapi-webscrapping-pdfextractor-production.up.railway.app)
- [API Documentation](https://fastapi-webscrapping-pdfextractor-production.up.railway.app/docs)
- [Alternative Docs](https://fastapi-webscrapping-pdfextractor-production.up.railway.app/redoc)
- [DA Price Monitoring](https://www.da.gov.ph/price-monitoring/)
- [CloudAMQP Dashboard](https://customer.cloudamqp.com/)

---

**Version:** 7.0.0  
**Last Updated:** December 2025
