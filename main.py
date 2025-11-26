# ==============================================================================
# DA PRICE INDEX SCRAPER - RULE-BASED PARSER WITH MULTI-LINE BUFFERING
# ==============================================================================
#
# Purpose: Scrapes and parses Daily Price Index PDFs from DA Philippines website
# Features:
#   - Smart multi-line buffering for commodity names
#   - Brand prioritization for cooking oils
#   - Automatic date detection from PDF filenames
#   - Market extraction and structured data output
#
# ==============================================================================

import logging
from fastapi import FastAPI, HTTPException, Depends, File, UploadFile
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
import httpx
from bs4 import BeautifulSoup
from io import BytesIO
from pypdf import PdfReader
from datetime import datetime
import re
from urllib.parse import urljoin
from typing import Optional, Dict, Any, List

# ==============================================================================
# CONFIGURATION
# ==============================================================================

BASE_URL = "https://www.da.gov.ph"
TARGET_URL = "https://www.da.gov.ph/price-monitoring/"
SHARED_SECRET = "Jeremiah_Madronio_API_Key_82219800JeremiahPux83147"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# ==============================================================================
# DATA MODELS (DTOs)
# ==============================================================================

class PriceRow(BaseModel):
    """Individual commodity price entry"""
    category: str = Field(..., description="Clean category name")
    commodity: str = Field(..., description="Normalized commodity name")
    origin: Optional[str] = Field(None, description="Local or Imported")
    unit: Optional[str] = Field(None, description="kg, pc, L, ml, bottle")
    price: Optional[float] = Field(None, description="Price per unit")

class PdfResponseStructured(BaseModel):
    """Complete API response structure"""
    status: str
    date_processed: Optional[str] = None
    original_url: str
    covered_markets: List[str]
    price_data: List[PriceRow]

class ScrapeRequest(BaseModel):
    """Request body for scraping endpoint"""
    target_url: str = Field(TARGET_URL)

# ==============================================================================
# APP INITIALIZATION & SECURITY
# ==============================================================================

app = FastAPI(title="DA Price Index Scraper (Buffered)", version="5.3.0")
api_key_header = APIKeyHeader(name="X-Internal-Secret", auto_error=False)

def verify_internal_access(x_internal_secret: str = Depends(api_key_header)):
    """Validates API key for all protected endpoints"""
    if x_internal_secret == SHARED_SECRET:
        return True
    raise HTTPException(status_code=401, detail="Unauthorized")

# ==============================================================================
# CATEGORY DEFINITIONS
# ==============================================================================
# Known categories from DA Price Index PDFs - used for accurate categorization

KNOWN_CATEGORIES = [
    "IMPORTED COMMERCIAL RICE", "LOCAL COMMERCIAL RICE", "CORN PRODUCTS",
    "FISH PRODUCTS", "BEEF MEAT PRODUCTS", "PORK MEAT PRODUCTS",
    "OTHER LIVESTOCK MEAT PRODUCTS", "POULTRY PRODUCTS",
    "LOWLAND VEGETABLES", "HIGHLAND VEGETABLES", "SPICES",
    "FRUITS", "OTHER BASIC COMMODITIES"
]

# ==============================================================================
# PDF EXTRACTION UTILITIES
# ==============================================================================

def extract_pdf_content(pdf_bytes: bytes) -> str:
    """
    Extracts raw text from PDF file

    Args:
        pdf_bytes: PDF file as bytes

    Returns:
        Combined text from all pages
    """
    pdf_file = BytesIO(pdf_bytes)
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            text += f"\n{extracted}\n"
    return text

def parse_date_from_filename(filename: str) -> Optional[datetime]:
    """
    Extracts date from PDF filename (e.g., "January-15-2024.pdf")

    Args:
        filename: PDF filename string

    Returns:
        datetime object if date found, None otherwise
    """
    match = re.search(r"([A-Za-z]+-\d{1,2}-\d{4})", filename)
    if not match:
        return None

    date_str = match.group(1)

    # Try different date formats (with/without leading zero)
    for fmt in ["%B-%d-%Y", "%b-%d-%Y", "%B-%#d-%Y", "%b-%#d-%Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None

# ==============================================================================
# COMMODITY NORMALIZATION LOGIC
# ==============================================================================

def normalize_oil_data(full_text: str) -> tuple[str, str]:
    """
    Smart parser for cooking oil with brand prioritization

    Priority Order: Jolly > Minola > Spring > Generic (Palm/Coconut)
    This ensures branded oils don't get merged with generic types

    Args:
        full_text: Raw text containing oil description

    Returns:
        tuple: (clean_name, unit)
        Example: ("Cooking Oil (Jolly)", "1 L")
    """
    upper = full_text.upper()

    # Step 1: Identify Brand/Type (Brands have priority!)
    oil_type = "Generic"

    if "MINOLA" in upper:
        oil_type = "Minola"
    elif "SPRING" in upper:
        oil_type = "Spring"
    elif "JOLLY" in upper:
        oil_type = "Jolly"
    elif "PALM" in upper:
        oil_type = "Palm"
    elif "COCONUT" in upper:
        oil_type = "Coconut"

    # Step 2: Identify Volume/Unit
    unit = "L"  # Default

    if "350" in upper:
        unit = "350 ml"
    elif "500" in upper:
        unit = "500 ml"
    elif "1,000" in upper or "1000" in upper or "1 LITER" in upper or "1L" in upper:
        unit = "1 L"

    # Step 3: Build clean name
    clean_name = f"Cooking Oil ({oil_type})" if oil_type != "Generic" else "Cooking Oil"

    return clean_name, unit

def normalize_commodity_name(full_text: str, category: str) -> tuple[str, Optional[str]]:
    """
    Main commodity name normalizer - handles multi-line names and special cases

    This function:
    1. Cleans garbage characters and extra spaces
    2. Applies category-specific rules (rice, vegetables, meat, etc.)
    3. Removes brand names, units, and noise words
    4. Returns standardized commodity name + unit override if applicable

    Args:
        full_text: Complete concatenated text (may include multiple lines)
        category: Current category context

    Returns:
        tuple: (clean_commodity_name, unit_override)
    """
    # Clean control characters and normalize spacing
    text_clean = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', full_text)
    text_clean = " ".join(text_clean.split())

    upper_cat = category.upper()
    upper_name = text_clean.upper()

    # -------------------------------------------------------------------------
    # SPECIAL HANDLERS FOR SPECIFIC COMMODITIES
    # -------------------------------------------------------------------------

    # Cooking Oil - uses brand-aware parser
    if "OTHER BASIC" in upper_cat and "COOKING OIL" in upper_name:
        return normalize_oil_data(text_clean)

    # -------------------------------------------------------------------------
    # CATEGORY-SPECIFIC NORMALIZATION RULES
    # -------------------------------------------------------------------------

    unit_override = None

    # Rice varieties
    if "RICE" in upper_cat:
        if "SPECIAL" in upper_name: return "Special White Rice", "kg"
        if "PREMIUM" in upper_name: return "Premium Rice", "kg"
        if "WELL MILLED" in upper_name: return "Well Milled Rice", "kg"
        if "REGULAR MILLED" in upper_name: return "Regular Milled Rice", "kg"
        if "GLUTINOUS" in upper_name: return "Glutinous Rice", "kg"
        if "JASPONICA" in upper_name or "JAPONICA" in upper_name: return "Jasponica Rice", "kg"

    # Vegetables - differentiate by color/type
    if "ONION" in upper_name:
        if "RED" in upper_name: return "Red Onion", "kg"
        if "WHITE" in upper_name: return "White Onion", "kg"

    if "BELL PEPPER" in upper_name:
        if "RED" in upper_name: return "Bell Pepper Red", "kg"
        if "GREEN" in upper_name: return "Bell Pepper Green", "kg"

    if "CHILLI" in upper_name or "SILING" in upper_name:
        if "RED" in upper_name or "LABUYO" in upper_name or "TINGALA" in upper_name:
            return "Chilli Red", "kg"
        if "GREEN" in upper_name:
            return "Chilli Green", "kg"

    # Fish and Meat products
    if "BANGUS" in upper_name: return "Bangus", "kg"
    if "TILAPIA" in upper_name: return "Tilapia", "kg"
    if "GALUNGGONG" in upper_name: return "Galunggong", "kg"
    if "PORK BELLY" in upper_name: return "Pork Belly", "kg"
    if "PORK CHOP" in upper_name: return "Pork Chop", "kg"
    if "WHOLE CHICKEN" in upper_name: return "Whole Chicken", "kg"
    if "EGG" in upper_name and "CHICKEN" in upper_name: return "Chicken Egg", "pc"
    if "TAMBAKOL" in upper_name or "YELLOW-FIN" in upper_name:
        return "Tambakol (Yellow-Fin Tuna)", "kg"

    # -------------------------------------------------------------------------
    # FALLBACK CLEANING FOR UNMATCHED COMMODITIES
    # -------------------------------------------------------------------------

    name = text_clean

    # Remove brand names and common noise words
    remove_words = [
        "Magnolia", "Bounty Fresh", "Unbranded", "Fresh", "Fully Dressed",
        "Jolly Brand", "Jolly", "Palm Olein", "Spring", "Minola", "Brand",
        "Local", "Imported", "frozen", "chilled", "whole round", "medium", "large",
        "suprema variety", "native"
    ]
    for word in remove_words:
        name = re.sub(rf'\b{word}\b', '', name, flags=re.IGNORECASE)

    # Remove unit measurements embedded in text
    name = re.sub(r'[\d,\.]+\s*[-]?\s*(ml|liter|l|kg|g|pc|bottle|bundles)[s]?', '', name, flags=re.IGNORECASE)

    # Remove parenthetical unit descriptions like "(500g)" or "(5-10 cm)"
    name = re.sub(r'\([0-9\s\-\.\,]+(g|kg|pc|cm|gram).*\)', '', name, flags=re.IGNORECASE)

    # Clean trailing/leading punctuation and extra spaces
    name = name.strip(" )/-,.")

    return " ".join(name.split()), unit_override

# ==============================================================================
# MAIN PARSING FUNCTION
# ==============================================================================

def parse_text_to_json(raw_text: str) -> Dict[str, Any]:
    """
    Converts raw PDF text into structured price data

    This is the core parser that:
    1. Extracts covered markets list
    2. Detects category headers
    3. Uses line buffering to handle multi-line commodity names
    4. Matches prices to their commodities
    5. Normalizes all data into clean JSON structure

    Args:
        raw_text: Complete PDF text content

    Returns:
        Dictionary with 'covered_markets' and 'price_data' keys
    """
    lines = raw_text.split('\n')
    price_data_list = []
    current_category = "UNKNOWN"
    market_list = []

    # Multi-line buffering system
    # Stores lines that are part of a commodity name but haven't reached price yet
    name_buffer = []

    # -------------------------------------------------------------------------
    # Step 1: Extract Covered Markets
    # -------------------------------------------------------------------------
    market_match = re.search(
        r"(?:d\)|Covered markets:)\s*(1\..+?)(?:Page|\Z)",
        raw_text,
        re.DOTALL | re.IGNORECASE
    )

    if market_match:
        raw_block = market_match.group(1)
        raw_markets = re.split(r'\s*\d+\.\s*', raw_block)
        market_list = [re.sub(r'[\n\r]', ' ', m).strip() for m in raw_markets if len(m) > 3]
        market_list = list(dict.fromkeys(market_list))  # Remove duplicates

    # -------------------------------------------------------------------------
    # Step 2: Process Each Line
    # -------------------------------------------------------------------------
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Check if line is a category header
        is_category = False
        for cat in KNOWN_CATEGORIES:
            if cat in line.upper():
                current_category = cat
                is_category = True
                name_buffer = []  # Reset buffer when entering new category
                break

        if is_category:
            continue

        # Skip known header/footer lines
        if any(x in line for x in ["Source:", "Note:", "Prevailing", "Retail Price",
                                   "Page", "Department of", "COMMODITY"]):
            continue

        # Only process lines within a known category
        if current_category != "UNKNOWN":

            # Look for price pattern at end of line
            price_match = re.search(r'(?:^|\s)(\d{1,3}(?:,\d{3})*\.\d{2}|\$n/a\$|-)\s*$', line)

            if price_match:
                # =====================================================
                # PRICE FOUND - Process Complete Row
                # =====================================================

                price_str = price_match.group(1).replace(',', '')
                line_content = line[:price_match.start()].strip()

                # Combine buffered lines with current line to get FULL commodity name
                full_raw_text = " ".join(name_buffer + [line_content])

                # Normalize the complete text
                clean_name, unit_override = normalize_commodity_name(full_raw_text, current_category)

                # Determine origin (Local vs Imported)
                origin = "Imported" if "IMPORTED" in full_raw_text.upper() or "IMPORTED" in current_category else "Local"

                # Determine unit (use override if specified, otherwise use defaults)
                unit = unit_override
                if not unit:
                    if "egg" in clean_name.lower():
                        unit = "pc"
                    elif "cooking oil" in current_category.lower():
                        unit = "L"
                    else:
                        unit = "kg"

                # Parse price value
                final_price = None
                try:
                    if price_str not in ['-', '$n/a$']:
                        final_price = float(price_str)
                except:
                    pass

                # Clean category name (remove LOCAL/IMPORTED prefix)
                clean_cat = current_category.replace("IMPORTED ", "").replace("LOCAL ", "").strip()

                # Save valid row
                if clean_name and len(clean_name) > 2 and clean_name.lower() != "or":
                    price_data_list.append(PriceRow(
                        category=clean_cat,
                        commodity=clean_name,
                        origin=origin,
                        unit=unit,
                        price=final_price
                    ))

                # Clear buffer after successfully processing row
                name_buffer = []

            else:
                # =====================================================
                # NO PRICE FOUND - Add to Buffer
                # =====================================================
                # This handles cases where commodity name spans multiple lines
                # Example: Line 1: "Cooking Oil (Palm..."
                #          Line 2: "Olein) 1 Liter 145.50"

                if len(line) > 1:
                    name_buffer.append(line)

    # -------------------------------------------------------------------------
    # Step 3: Return Structured Data
    # -------------------------------------------------------------------------
    return {
        "covered_markets": market_list,
        "price_data": price_data_list
    }

# ==============================================================================
# API ENDPOINTS
# ==============================================================================

@app.post("/api/scrape-new-pdf", response_model=PdfResponseStructured, dependencies=[Depends(verify_internal_access)])
async def scrape_new_pdf_data(request: ScrapeRequest):
    """
    Scrapes the newest Daily Price Index PDF from DA website

    Process:
    1. Fetches HTML from DA price monitoring page
    2. Finds all DPI PDF links
    3. Determines newest PDF by date in filename
    4. Downloads and parses that PDF
    5. Returns structured price data

    Requires: X-Internal-Secret header for authentication
    """
    async with httpx.AsyncClient(timeout=30) as client:
        # Fetch price monitoring page
        try:
            resp = await client.get(request.target_url, headers=HEADERS)
            resp.raise_for_status()
        except Exception as e:
            raise HTTPException(500, f"Fetch failed: {str(e)}")

        # Parse HTML and find PDF links
        soup = BeautifulSoup(resp.text, 'lxml')
        links = soup.find_all('a', href=re.compile(r'(Daily-Price-Index|DPI).*?\.pdf$', re.IGNORECASE))

        if not links:
            raise HTTPException(404, "No Daily Price Index PDFs found.")

        # Find newest PDF by date
        newest_link = None
        latest_date = datetime.min

        for link in links:
            href = link.get('href')
            f_name = href.split('/')[-1]
            f_date = parse_date_from_filename(f_name)

            if f_date and f_date > latest_date:
                latest_date = f_date
                newest_link = {
                    'href': urljoin(BASE_URL, href),
                    'date_str': f_date.strftime("%Y-%m-%d")
                }

        if not newest_link:
            raise HTTPException(404, "Could not determine dates from PDF links.")

        # Download and parse PDF
        print(f"Processing: {newest_link['href']}")
        pdf_resp = await client.get(newest_link['href'], headers=HEADERS)
        content = extract_pdf_content(pdf_resp.content)
        data = parse_text_to_json(content)

        # Return structured response
        return PdfResponseStructured(
            status="Success",
            date_processed=newest_link['date_str'],
            original_url=newest_link['href'],
            covered_markets=data['covered_markets'],
            price_data=data['price_data']
        )

@app.post("/api/extract-manual", response_model=PdfResponseStructured, dependencies=[Depends(verify_internal_access)])
async def extract_manual_pdf(file: UploadFile = File(...)):
    """
    Manually upload and parse a DPI PDF file

    Useful for:
    - Processing archived PDFs
    - Testing with specific documents
    - Extracting data from downloaded files

    Requires: X-Internal-Secret header for authentication
    """
    if file.content_type != 'application/pdf':
        raise HTTPException(400, "File must be PDF")

    # Read and parse uploaded file
    content = await file.read()
    text = extract_pdf_content(content)
    date_str = datetime.now().strftime("%Y-%m-%d")
    data = parse_text_to_json(text)

    # Return structured response
    return PdfResponseStructured(
        status="Success (Manual)",
        date_processed=date_str,
        original_url=f"Manual: {file.filename}",
        covered_markets=data['covered_markets'],
        price_data=data['price_data']
    )

@app.get("/")
def root():
    """Health check endpoint"""
    return {"message": "Smart DA Price Scraper is Running"}