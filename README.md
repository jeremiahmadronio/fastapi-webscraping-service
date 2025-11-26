# 🌾 DA Price Index Scraper

A FastAPI-based web scraper that extracts and parses Daily Price Index (DPI) PDFs from the Department of Agriculture Philippines website. Features intelligent multi-line buffering, brand-aware commodity normalization, and automatic market detection.

## 🚀 Live API

**Base URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app`

**Interactive Docs:** [/docs](https://fastapi-webscrapping-pdfextractor-production.up.railway.app/docs)

---

## ✨ Features

- 🔍 **Smart PDF Parsing** - Multi-line buffering for commodity names spanning multiple lines
- 🏷️ **Brand Prioritization** - Intelligent handling of branded vs generic cooking oils
- 📅 **Auto Date Detection** - Extracts dates from PDF filenames automatically
- 🏪 **Market Extraction** - Automatically identifies covered markets from PDFs
- 🧹 **Data Normalization** - Cleans and standardizes commodity names, units, and categories
- 🔐 **API Key Authentication** - Secured endpoints with header-based authentication

---

## 📋 API Endpoints

### 1. Health Check

```http
GET /
```

**Response:**
```json
{
  "message": "Smart DA Price Scraper is Running"
}
```

---

### 2. Scrape Latest PDF

Automatically fetches and parses the newest Daily Price Index PDF from DA website.

```http
POST /api/scrape-new-pdf
```

**Headers:**
```
X-Internal-Secret: <YOUR_API_KEY>
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
  "date_processed": "2025-11-26",
  "original_url": "https://www.da.gov.ph/...",
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
    },
    {
      "category": "VEGETABLES",
      "commodity": "Red Onion",
      "origin": "Local",
      "unit": "kg",
      "price": 85.00
    }
  ]
}
```

---

### 3. Manual PDF Upload

Upload and parse any DPI PDF file manually.

```http
POST /api/extract-manual
```

**Headers:**
```
X-Internal-Secret: <YOUR_API_KEY>
```

**Body:** `multipart/form-data`
- **Key:** `file`
- **Value:** PDF file

**Response:** Same structure as `/api/scrape-new-pdf`

---

## 🔑 Authentication

All endpoints (except root `/`) require authentication via the `X-Internal-Secret` header.

**To obtain an API key, contact the administrator.**

---

## 🛠️ Local Development

### Prerequisites

- Python 3.10+
- pip

### Installation

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd fastapi-webscrapping-pdf_extractor
```

2. **Create virtual environment:**
```bash
python -m venv .venv
```

3. **Activate virtual environment:**

**Windows:**
```bash
.\.venv\Scripts\Activate.ps1
```

**Mac/Linux:**
```bash
source .venv/bin/activate
```

4. **Install dependencies:**
```bash
pip install -r requirements.txt
```

### Running Locally

```bash
uvicorn main:app --reload
```

Server will start at `http://localhost:8000`

- **API Docs:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

---

## 📦 Dependencies

- **FastAPI** - Modern web framework
- **Uvicorn** - ASGI server
- **httpx** - Async HTTP client for web scraping
- **BeautifulSoup4** - HTML parsing
- **pypdf** - PDF text extraction
- **Pydantic** - Data validation
- **python-multipart** - File upload support

Full list in `requirements.txt`

---

## 🏗️ Project Structure

```
.
├── main.py                 # FastAPI application & parsing logic
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

---

## 📊 Supported Categories

The scraper recognizes and normalizes the following DA categories:

- Commercial Rice (Imported/Local)
- Corn Products
- Fish Products
- Meat Products (Beef, Pork, Poultry, Others)
- Vegetables (Lowland, Highland)
- Spices
- Fruits
- Other Basic Commodities

---

## 🧪 Testing with Postman

### Example: Scrape Latest PDF

1. **Method:** POST
2. **URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/scrape-new-pdf`
3. **Headers:**
   - `X-Internal-Secret`: `<YOUR_API_KEY>`
   - `Content-Type`: `application/json`
4. **Body (raw JSON):**
   ```json
   {
     "target_url": "https://www.da.gov.ph/price-monitoring/"
   }
   ```
5. **Click Send**

### Example: Manual Upload

1. **Method:** POST
2. **URL:** `https://fastapi-webscrapping-pdfextractor-production.up.railway.app/api/extract-manual`
3. **Headers:**
   - `X-Internal-Secret`: `<YOUR_API_KEY>`
4. **Body:** `form-data`
   - Key: `file` (Type: File)
   - Value: Select your PDF file
5. **Click Send**

---

## 🚢 Deployment

### Railway (Current)

Deployed on [Railway.app](https://railway.app) with automatic deployments on push to `main` branch.

**Environment Variables:**
- `PORT` - Auto-set by Railway

### Manual Deployment

To deploy elsewhere:

1. Ensure `requirements.txt` is up to date
2. Set `PORT` environment variable if needed
3. Run: `uvicorn main:app --host 0.0.0.0 --port $PORT`

---

## 🔧 Configuration

Key configurations in `main.py`:

```python
BASE_URL = "https://www.da.gov.ph"
TARGET_URL = "https://www.da.gov.ph/price-monitoring/"
```

To change target source, modify these constants.

---

## 🐛 Troubleshooting

### Issue: "Application failed to respond"
- Check Railway logs for errors
- Ensure `PORT` environment variable is properly configured

### Issue: "No Daily Price Index PDFs found"
- DA website structure may have changed
- Verify PDF link patterns in source code

### Issue: "Unauthorized" response
- Ensure `X-Internal-Secret` header is included
- Verify API key is correct

---

## 📝 Notes

- PDFs are **not stored** - only parsed in-memory
- Parser uses **rule-based logic** optimized for DA PDF format
- **Multi-line buffering** handles commodity names split across lines
- **Brand-aware** normalization prevents mixing branded and generic items

---

## 👤 Author

**Jeremiah Madronio**

---

## 📄 License

Private Project - All Rights Reserved

---

## 🤝 Contributing

This is a private project. For feature requests or bug reports, contact the administrator.

---

## 📞 Support

For API access or technical support, contact the project administrator.
