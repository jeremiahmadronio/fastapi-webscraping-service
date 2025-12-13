# ==============================================================================
# DA PRICE INDEX SCRAPER - IMPROVED RULE-BASED PARSER
# ==============================================================================
#
# Key Improvements:
#   1. Better multi-line commodity detection using lookahead
#   2. Smarter unit extraction from specification column
#   3. More robust brand and noise word removal
#   4. Better handling of parenthetical descriptions
#   5. Improved price detection to avoid false matches
#
# ==============================================================================

import logging
from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel, Field
import httpx
from bs4 import BeautifulSoup
from io import BytesIO
from pypdf import PdfReader
from datetime import datetime
import re
from urllib.parse import urljoin
from typing import Optional, Dict, Any, List


app = FastAPI(title="DA Price Index Scraper (RabbitMQ Ready)", version="7.0.0")


# ==============================================================================
# CONFIGURATION
# ==============================================================================

BASE_URL = "https://www.da.gov.ph"
TARGET_URL = "https://www.da.gov.ph/price-monitoring/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

# ==============================================================================
# DATA MODELS
# ==============================================================================

class PriceRow(BaseModel):
    """Individual commodity price entry"""
    category: str = Field(..., description="Clean category name")
    commodity: str = Field(..., description="Normalized commodity name")
    origin: Optional[str] = Field(None, description="Local or Imported")
    unit: str = Field("kg", description="kg, pc, L, ml")
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
# CATEGORY DEFINITIONS
# ==============================================================================

KNOWN_CATEGORIES = [
    "IMPORTED COMMERCIAL RICE", "LOCAL COMMERCIAL RICE", "CORN PRODUCTS",
    "FISH PRODUCTS", "BEEF MEAT PRODUCTS", "PORK MEAT PRODUCTS",
    "OTHER LIVESTOCK MEAT PRODUCTS", "POULTRY PRODUCTS",
    "LOWLAND VEGETABLES", "HIGHLAND VEGETABLES", "SPICES",
    "FRUITS", "OTHER BASIC COMMODITIES"
]

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def extract_pdf_content(pdf_bytes: bytes) -> str:
    """Extracts raw text from PDF"""
    pdf_file = BytesIO(pdf_bytes)
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            text += f"\n{extracted}\n"
    return text

def parse_date_from_filename(filename: str) -> Optional[datetime]:
    """Extracts date from filename like 'December-10-2025-DPI-AFC.pdf'"""
    match = re.search(r"([A-Za-z]+-\d{1,2}-\d{4})", filename)
    if not match:
        return None

    date_str = match.group(1)
    for fmt in ["%B-%d-%Y", "%b-%d-%Y", "%B-%#d-%Y", "%b-%#d-%Y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# ==============================================================================
# UNIT EXTRACTION
# ==============================================================================

def extract_unit_from_spec(spec_text: str, commodity_name: str) -> str:
    """
    Extracts unit from specification column
    Returns: 'kg', 'pc', 'L', or 'ml'
    """
    upper_spec = spec_text.upper()
    upper_name = commodity_name.upper()

    # Chicken eggs are always per piece
    if "EGG" in upper_name and "CHICKEN" in upper_name:
        return "pc"

    # Cooking oils - check for volume
    if "COOKING OIL" in upper_name:
        if "350" in upper_spec and "ML" in upper_spec:
            return "350 ml"
        if "500" in upper_spec and "ML" in upper_spec:
            return "500 ml"
        if "1" in upper_spec and ("LITER" in upper_spec or "L" in upper_spec):
            return "1 L"
        return "L"

    # Default for everything else
    return "kg"

# ==============================================================================
# COMMODITY NORMALIZATION
# ==============================================================================

def normalize_commodity_name(commodity_text: str, category: str) -> tuple[str, Optional[str]]:
    """
    Main normalization function
    Returns: (clean_name, specification)
    """
    # Clean control characters
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', commodity_text)
    text = " ".join(text.split())

    upper_text = text.upper()
    upper_cat = category.upper()

    # -------------------------------------------------------------------------
    # RICE PRODUCTS
    # -------------------------------------------------------------------------
    if "RICE" in upper_cat:
        if "BASMATI" in upper_text:
            return "Basmati Rice", None
        if "GLUTINOUS" in upper_text:
            return "Glutinous Rice", None
        if "JASPONICA" in upper_text or "JAPONICA" in upper_text:
            return "Jasponica Rice", None
        if "SPECIAL" in upper_text and "WHITE" in upper_text:
            return "Special White Rice", None
        if "PREMIUM" in upper_text:
            return "Premium Rice", "5% broken"
        if "WELL MILLED" in upper_text:
            return "Well Milled Rice", "1-19% bran streak"
        if "REGULAR MILLED" in upper_text:
            return "Regular Milled Rice", "20-40% bran streak"

    # -------------------------------------------------------------------------
    # CORN PRODUCTS
    # -------------------------------------------------------------------------
    if "CORN" in upper_cat:
        if "WHITE" in upper_text and "COB" in upper_text:
            return "Corn White", "Cob, Glutinous"
        if "YELLOW" in upper_text and "COB" in upper_text:
            return "Corn Yellow", "Cob, Sweet"
        if "GRITS" in upper_text and "WHITE" in upper_text and "FOOD" in upper_text:
            return "Corn Grits White", "Food Grade"
        if "GRITS" in upper_text and "YELLOW" in upper_text and "FOOD" in upper_text:
            return "Corn Grits Yellow", "Food Grade"
        if "CRACKED" in upper_text:
            return "Corn Cracked", "Feed Grade"
        if "GRITS" in upper_text and "FEED" in upper_text:
            return "Corn Grits", "Feed Grade"

    # -------------------------------------------------------------------------
    # FISH PRODUCTS
    # -------------------------------------------------------------------------
    if "FISH" in upper_cat:
        # Extract size specification
        size_match = re.search(r'(Large|Medium|Small).*?(\d+-?\d*\s*pcs?/?kg)?', text, re.IGNORECASE)
        size_spec = size_match.group(0) if size_match else None

        if "ALUMAHAN" in upper_text or "MACKEREL" in upper_text and "INDIAN" in upper_text:
            return "Alumahan (Indian Mackerel)", size_spec
        if "BANGUS" in upper_text:
            if "LARGE" in upper_text:
                return "Bangus Large",size_spec
            if "MEDIUM" in upper_text:
                return "Bangus Medium",size_spec
        if "BONITO" in upper_text:
            return "Bonito (Frigate Tuna)",size_spec
        if "GALUNGGONG" in upper_text:
            return "Galunggong", "Medium (12-14 pcs/kg)"
        if "MACKEREL" in upper_text and "INDIAN" not in upper_text:
            return "Mackerel", None
        if "PAMPANO" in upper_text:
            return "Pampano", None
        if "SALMON BELLY" in upper_text:
            return "Salmon Belly", None
        if "SALMON HEAD" in upper_text:
            return "Salmon Head", None
        if "SARDINES" in upper_text or "TAMBAN" in upper_text:
            return "Sardines (Tamban)", None
        if "SQUID" in upper_text or "PUSIT" in upper_text:
            return "Squid", size_spec
        if "TAMBAKOL" in upper_text or "YELLOW-FIN" in upper_text:
            return "Tambakol (Yellow-Fin Tuna)", "Medium"
        if "TILAPIA" in upper_text:
            return "Tilapia", "Medium (5-6 pcs/kg)"

    # -------------------------------------------------------------------------
    # MEAT PRODUCTS (BEEF, PORK, POULTRY)
    # -------------------------------------------------------------------------
    if "BEEF" in upper_cat:
        # Extract size/type specification if present
        size_match = re.search(r'\b(Large|Medium|Small|Lean|Boneless|with Bones)\b', text, re.IGNORECASE)
        size_spec = size_match.group(0) if size_match else None

        # Check for specific cuts in order of specificity
        if "TENDERLOIN" in upper_text:
            return "Beef Tenderloin", size_spec
        if "STRIP" in upper_text and "LOIN" in upper_text:
            return "Beef Striploin", size_spec
        if "SIRLOIN" in upper_text:
            return "Beef Sirloin", size_spec
        if "SHORT RIB" in upper_text:
            return "Beef Short Ribs", size_spec
        if "RIB EYE" in upper_text:
            return "Beef Rib Eye", size_spec
        if "RIB SET" in upper_text:
            return "Beef Rib Set", size_spec
        if "RIB" in upper_text:
            return "Beef Ribs", size_spec
        if "RUMP" in upper_text:
            return "Beef Rump", size_spec
        if "ROUND" in upper_text:
            return "Beef Round", size_spec
        if "LOIN" in upper_text:
            return "Beef Loin", size_spec
        if "PLATE" in upper_text:
            return "Beef Plate", size_spec
        if "CHUCK" in upper_text:
            return "Beef Chuck", size_spec
        if "BRISKET" in upper_text:
            return "Beef Brisket", size_spec
        if "SHANK" in upper_text:
            return "Beef Shank", size_spec

        # Fallback
        base_name = text
        base_name = re.sub(r'\b(Large|Medium|Small|Lean|Boneless|with Bones)\b', '', base_name, flags=re.IGNORECASE)
        base_name = base_name.strip(", ")
        return base_name if len(base_name) > 2 else "Beef", size_spec



    if "PORK" in upper_cat:
        base_name = text
        base_name = re.sub(r'\b(Local|Imported|Liempo|Kasim)\b', '', base_name, flags=re.IGNORECASE)
        # Keep common name in parenthesis
        if "BELLY" in upper_text:
            return "Pork Belly (Liempo)", None
        if "PICNIC SHOULDER" in upper_text:
            return "Pork Picnic Shoulder (Kasim)", None
        base_name = base_name.strip(", ")
        return base_name, None

    if "POULTRY" in upper_cat:
        # Extract brand if present
        brand = None
        if "MAGNOLIA" in upper_text:
            brand = "Magnolia"
        elif "BOUNTY FRESH" in upper_text:
            brand = "Bounty Fresh"
        elif "UNBRANDED" in upper_text:
            brand = "Unbranded"

        # Remove brand from name
        base_name = re.sub(r'\b(Magnolia|Bounty Fresh|Unbranded|Fresh|Fully Dressed)\b', '', text, flags=re.IGNORECASE)
        base_name = base_name.strip(", ")

        # Handle chicken egg specially
        if "EGG" in upper_text:
            return "Chicken Egg", "Medium (56-60 grams/pc)"

        return base_name, brand

    # -------------------------------------------------------------------------
    # VEGETABLES
    # -------------------------------------------------------------------------
    if "VEGETABLE" in upper_cat:
        # Extract size/bundle specification - IMPROVED PATTERN
        # Matches: "Medium (8-10 cm diameter/bunch hd)", "510 gm - 1 kg/head", "8-10 pcs/kg", etc.
        spec_match = re.search(
            r'((?:Medium|Large|Small)?\s*\(?\d+-?\d*\s*(?:cm|gm?|g|pcs)(?:\s*[-/]\s*\d+\s*(?:kg|cm|g|gm))?\s*(?:diameter|bunch hd|head|pcs/kg)?[)]?)',
            text,
            re.IGNORECASE
        )
        spec = spec_match.group(1).strip() if spec_match else None

        base_name = text
        # Remove specifications from name - IMPROVED CLEANUP
        base_name = re.sub(
            r'(?:Medium|Large|Small)?\s*\(?\d+-?\d*\s*(?:cm|gm?|g|pcs)(?:\s*[-/]\s*\d+\s*(?:kg|cm|g|gm))?\s*(?:diameter|bunch hd|head|pcs/kg)?[)]?',
            '',
            base_name,
            flags=re.IGNORECASE
        )
        base_name = re.sub(r'\b(Local|Imported|Native|Suprema Variety|Medium|Large|Small)\b', '', base_name, flags=re.IGNORECASE)

        # Clean up leftover parentheses and commas
        base_name = re.sub(r'\(\s*\)', '', base_name)
        base_name = re.sub(r'\s+', ' ', base_name)

        # Handle specific vegetables
        if "BELL PEPPER" in upper_text:
            if "GREEN" in upper_text:
                return "Bell Pepper (Green)", spec
            if "RED" in upper_text:
                return "Bell Pepper (Red)", spec
            return "Bell Pepper", spec

        if "CABBAGE" in upper_text:
            if "RARE BALL" in upper_text:
                return "Cabbage (Rare Ball)", spec
            if "SCORPIO" in upper_text:
                return "Cabbage (Scorpio)", spec
            if "WONDER BALL" in upper_text:
                return "Cabbage (Wonder Ball)", spec
            return "Cabbage", spec

        if "LETTUCE" in upper_text:
            if "GREEN ICE" in upper_text:
                return "Lettuce (Green Ice)", spec
            if "ICEBERG" in upper_text:
                return "Lettuce (Iceberg)", spec
            if "ROMAINE" in upper_text:
                return "Lettuce (Romaine)", spec
            return "Lettuce", spec

        # Handle other specific vegetables
        if "BROCCOLI" in upper_text:
            return "Broccoli", spec

        if "POTATO" in upper_text:
            return "White Potato", spec
        if "CAULIFLOWER" in upper_text:
            return "Cauliflower", spec
        if "CARROTS" in upper_text or "CARROT" in upper_text:
            return "Carrots", spec
        if "CELERY" in upper_text:
            return "Celery", spec
        if "CHAYOTE" in upper_text:
            return "Chayote", spec
        if "HABICHUELAS" in upper_text or "BAGUIO BEANS" in upper_text:
            return "Baguio Beans", spec
        if "PECHAY" in upper_text and "BAGUIO" in upper_text:
            return "Pechay Baguio", spec

        base_name = base_name.strip(", ()")
        return base_name, spec

    # -------------------------------------------------------------------------
    # SPICES
    # -------------------------------------------------------------------------
    if "SPICE" in upper_cat:
        if "CHILLI" in upper_text or "CHILI" in upper_text:
            if "RED" in upper_text or "TINGALA" in upper_text:
                return "Chilli Red", "Tingala"
            if "GREEN" in upper_text:
                return "Chilli Green", "Haba/Panigang"
            if "TIGER" in upper_text:
                return "Tiger Chillies", None

        if "GARLIC" in upper_text:
            if "NATIVE" in upper_text:
                return "Garlic Native", None
            return "Garlic", None

        if "GINGER" in upper_text:
            return "Ginger", "Medium (150-300 gm)"

        if "ONION" in upper_text:
            size_spec = None
            if "MEDIUM" in upper_text:
                size_spec = "Medium"
            if "LARGE" in upper_text:
                size_spec = "Large"

            if "RED" in upper_text:
                return "Red Onion", size_spec
            if "WHITE" in upper_text:
                return "White Onion", size_spec

    # -------------------------------------------------------------------------
    # FRUITS
    # -------------------------------------------------------------------------
    if "FRUIT" in upper_cat:
        spec_match = re.search(r'(Ripe|Green|Solo|\d+-\d+\s*pcs/kg)', text, re.IGNORECASE)
        spec = spec_match.group(1) if spec_match else None

        base_name = text
        base_name = re.sub(r'\b(Ripe|Green|Solo|\d+-\d+\s*pcs/kg)\b', '', base_name, flags=re.IGNORECASE)

        if "BANANA" in upper_text:
            if "LAKATAN" in upper_text:
                return "Banana (Lakatan)", "8-10 pcs/kg"
            if "LATUNDAN" in upper_text:
                return "Banana (Latundan)", "10-12 pcs/kg"
            if "SABA" in upper_text:
                return "Banana (Saba)", None

        if "MANGO" in upper_text and "CARABAO" in upper_text:
            return "Mango (Carabao)", "Ripe, 3-4 pcs/kg"

        if "PAPAYA" in upper_text:
            return "Papaya", "Solo, Ripe, 2-3 pcs/kg"

        base_name = base_name.strip(", ()")
        return base_name, spec

    # -------------------------------------------------------------------------
    # OTHER BASIC COMMODITIES
    # -------------------------------------------------------------------------
    if "BASIC" in upper_cat:
        # Cooking Oil
        if "COOKING OIL" in upper_text:
            brand_type = "Palm"  # default

            if "COCONUT" in upper_text:
                brand_type = "Coconut"
            elif "MINOLA" in upper_text:
                brand_type = "Minola"
            elif "SPRING" in upper_text:
                brand_type = "Spring"
            elif "JOLLY" in upper_text or "PALM OLEIN" in upper_text:
                brand_type = "Palm Olein (Jolly)"

            # Volume is extracted separately
            return f"Cooking Oil ({brand_type})", None

        # Sugar
        if "SUGAR" in upper_text:
            if "REFINED" in upper_text:
                return "Sugar (Refined)", None
            if "WASHED" in upper_text:
                return "Sugar (Washed)", None
            if "BROWN" in upper_text:
                return "Sugar (Brown)", None

        # Salt
        if "SALT" in upper_text:
            if "IODIZED" in upper_text:
                return "Salt (Iodized)", None
            if "ROCK" in upper_text:
                return "Salt (Rock)", None

    # -------------------------------------------------------------------------
    # FALLBACK: Clean generic commodities
    # -------------------------------------------------------------------------
    base_name = text
    base_name = re.sub(r'\b(Local|Imported|Fresh|Frozen|Chilled|Whole Round|Native)\b', '', base_name, flags=re.IGNORECASE)
    base_name = re.sub(r'\d+[-]?\d*\s*(?:pcs?/?kg|grams?|cm|ml|L)', '', base_name, flags=re.IGNORECASE)
    base_name = base_name.strip(", ()")

    return base_name, None

# ==============================================================================
# MAIN PARSING FUNCTION
# ==============================================================================

def parse_text_to_json(raw_text: str) -> Dict[str, Any]:
    """
    Improved parser with better multi-line handling
    """
    lines = raw_text.split('\n')
    price_data_list = []
    current_category = "UNKNOWN"
    market_list = []

    # Buffer system for multi-line commodities
    commodity_buffer = []
    specification_buffer = []

    # -------------------------------------------------------------------------
    # Extract Covered Markets
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
        market_list = list(dict.fromkeys(market_list))

    # -------------------------------------------------------------------------
    # Process Lines
    # -------------------------------------------------------------------------
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not line:
            i += 1
            continue

        # Check for category header
        is_category = False
        for cat in KNOWN_CATEGORIES:
            if cat in line.upper():
                current_category = cat
                is_category = True
                commodity_buffer = []
                specification_buffer = []
                break

        if is_category:
            i += 1
            continue

        # Skip headers/footers - IMPROVED DETECTION
        # Skip page indicators
        if re.search(r'Page\s+\d+\s+of\s+\d+', line, re.IGNORECASE):
            i += 1
            continue
        
        header_keywords = ["Source:", "Note:", "Department"]
        
        # Count how many header keywords are in the line
        header_count = sum(1 for kw in header_keywords if kw in line)
        
        # Skip if it's a header line (multiple keywords) or contains PREVAILING/COMMODITY
        if header_count >= 1 or "PREVAILING" in line or "COMMODITY" in line or "SPECIFICATION" in line or "PRICE PER UNIT" in line:
            i += 1
            continue

        if current_category == "UNKNOWN":
            i += 1
            continue

        # Look for price at end of line
        price_match = re.search(r'\s+(\d{1,3}(?:,\d{3})*\.\d{2}|n/a)\s*$', line)

        if price_match:
            # PRICE FOUND - Process complete row
            price_str = price_match.group(1).replace(',', '')
            line_content = line[:price_match.start()].strip()

            # =====================================================
            # SKIP PAGE INDICATORS
            # =====================================================
            if re.search(r'Page\s+\d+\s+of\s+\d+', line_content, re.IGNORECASE):
                commodity_buffer = []
                specification_buffer = []
                i += 1
                continue

            # =====================================================
            # CHECK IF THIS IS A HEADER ROW - EARLY SKIP
            # =====================================================
            # Count header keywords in the line
            header_keywords = ["PREVAILING", "RETAIL", "PRICE", "COMMODITY", "SPECIFICATION", "UNIT", "P/UNIT"]
            header_keyword_count = sum(1 for kw in header_keywords if kw in line_content.upper())

            # If 2+ header keywords found, OR contains "RETAIL PRICE PER", this is a header row - SKIP
            if header_keyword_count >= 2 or "RETAIL PRICE PER" in line_content.upper() or "PREVAILING RETAIL" in line_content.upper():
                commodity_buffer = []
                specification_buffer = []
                i += 1
                continue

            # =====================================================
            # AGGRESSIVE HEADER PHRASE REMOVAL
            # =====================================================
            # Remove all variations of the header phrase
            original_content = line_content
            
            # Remove "RETAIL PRICE PER" (with or without UNIT, with or without PREVAILING)
            line_content = re.sub(
                r'(?:PREVAILING\s+)?(?:RETAIL\s+)?PRICE\s+PER(?:\s+UNIT)?',
                '',
                line_content,
                flags=re.IGNORECASE
            ).strip()

            # If nothing was removed and line still has too many header keywords, skip it
            if line_content == original_content and header_keyword_count >= 1:
                commodity_buffer = []
                specification_buffer = []
                i += 1
                continue

            # Remove remaining header keywords one by one
            line_content = re.sub(
                r'\b(PREVAILING|RETAIL|PRICE|PER|UNIT|COMMODITY|SPECIFICATION|PAGE|DEPARTMENT|COVERED|MARKETS|OF|P/UNIT)\b\s*',
                '',
                line_content,
                flags=re.IGNORECASE
            ).strip()

            # Clean up multiple spaces
            line_content = re.sub(r'\s+', ' ', line_content).strip()

            # If line_content is empty or only header remnants, skip
            if not line_content or len(line_content) < 3:
                commodity_buffer = []
                specification_buffer = []
                i += 1
                continue

            # Final validation - if line is still mostly garbage, skip
            garbage_words = ["RETAIL", "PRICE", "UNIT", "COMMODITY", "SPECIFICATION", "PREVAILING", "PAGE"]
            if all(word in line_content.upper() for word in garbage_words[:2]):
                commodity_buffer = []
                specification_buffer = []
                i += 1
                continue

            # =====================================================
            # EXTRACT ORIGIN FROM LINE CONTENT
            # =====================================================
            origin = "Local"
            if "IMPORTED" in line_content.upper() or "IMPORTED" in current_category:
                origin = "Imported"

            # Remove origin keywords from line_content
            line_content_clean = re.sub(
                r',?\s*\b(Local|Imported)\b',
                '',
                line_content,
                flags=re.IGNORECASE
            ).strip()

            # =====================================================
            # BUILD FULL COMMODITY TEXT
            # =====================================================
            if specification_buffer:
                full_spec = " ".join(specification_buffer + [line_content_clean])
            else:
                full_spec = line_content_clean

            full_commodity = " ".join(commodity_buffer) if commodity_buffer else ""

            # If both buffers are empty, use line_content_clean as commodity
            if not full_commodity:
                full_commodity = line_content_clean
                full_spec = ""

            # =====================================================
            # NORMALIZE COMMODITY NAME
            # =====================================================
            clean_name, specification = normalize_commodity_name(
                full_commodity,
                current_category
            )

            # Extract unit
            unit = extract_unit_from_spec(full_spec if full_spec else full_commodity, clean_name)

            # Parse price
            final_price = None
            try:
                if price_str not in ['n/a', '-']:
                    final_price = float(price_str)
            except:
                pass

            # Clean category
            clean_cat = current_category.replace("IMPORTED ", "").replace("LOCAL ", "").strip()

            # Save row - SKIP if price is n/a or commodity name is invalid
            if clean_name and len(clean_name) > 2 and final_price is not None:
                # Final blacklist check - skip known header remnants
                blacklist = ["PREVAILING", "PRICE", "UNIT", "COMMODITY", "SPECIFICATION", "PAGE", "RETAIL", "RETAIL PRICE PER", "PREVAILING RETAIL", "PRICE PER UNIT"]

                # Also check if clean_name contains multiple header keywords (likely header text)
                header_keywords_in_name = sum(1 for kw in ["RETAIL", "PRICE", "PER", "UNIT", "PREVAILING"] if kw in clean_name.upper())

                if clean_name.upper() not in blacklist and header_keywords_in_name < 2:
                    price_data_list.append(PriceRow(
                        category=clean_cat,
                        commodity=clean_name,
                        origin=origin,
                        unit=unit,
                        price=final_price
                    ))

            # Reset buffers
            commodity_buffer = []
            specification_buffer = []

        else:
            # NO PRICE - Check if this is commodity or specification
            # Heuristic: If previous line had no price and current line has no price,
            # treat first line as commodity, second as specification

            if not commodity_buffer:
                commodity_buffer.append(line)
            else:
                specification_buffer.append(line)

        i += 1

    return {
        "covered_markets": market_list,
        "price_data": price_data_list
    }


# ==============================================================================
# API ENDPOINTS
# ==============================================================================

@app.post("/api/scrape-new-pdf", response_model=PdfResponseStructured)
async def scrape_new_pdf_data(request: ScrapeRequest):
    """
    Scrapes the newest Daily Price Index PDF from DA website

    Process:
    1. Fetches HTML from DA price monitoring page
    2. Finds all DPI PDF links
    3. Determines newest PDF by date in filename
    4. Downloads and parses that PDF
    5. Returns structured price data

    Note: API key authentication removed - use RabbitMQ for secure communication
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

@app.post("/api/extract-manual", response_model=PdfResponseStructured)
async def extract_manual_pdf(file: UploadFile = File(...)):
    """
    Manually upload and parse a DPI PDF file

    Useful for:
    - Processing archived PDFs
    - Testing with specific documents
    - Extracting data from downloaded files

    Note: API key authentication removed - use RabbitMQ for secure communication
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
    return {"message": "Smart DA Price Scraper is Running (RabbitMQ Ready)"}


async def run_standalone_scraper():
    """
    Independent scraper function for RabbitMQ Worker
    """
    print(" [LOGIC] Starting standalone scrape...")

    # 1. Setup Client
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            # Fetch Page
            resp = await client.get(TARGET_URL, headers=HEADERS)
            resp.raise_for_status()

            # Find PDF Link
            soup = BeautifulSoup(resp.text, 'lxml')
            links = soup.find_all('a', href=re.compile(r'(Daily-Price-Index|DPI).*?\.pdf$', re.IGNORECASE))

            if not links:
                print(" [ERROR] No PDF links found.")
                return None

            # Get Newest
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
                return None

            # Download & Parse
            print(f" [LOGIC] Downloading: {newest_link['href']}")
            pdf_resp = await client.get(newest_link['href'], headers=HEADERS)
            content = extract_pdf_content(pdf_resp.content)
            data = parse_text_to_json(content)

            return {
                "date": newest_link['date_str'],
                "data": data
            }

        except Exception as e:
            print(f" [ERROR] Scraping failed: {e}")
            return None



# ==============================================================================
# RAILWAY DEPLOYMENT CONFIGURATION
# ==============================================================================

if __name__ == "__main__":
    import uvicorn
    import os

    # Get port from environment variable (Railway sets this automatically)
    port = int(os.environ.get("PORT", 8000))

    # Run the FastAPI app
    uvicorn.run(app, host="0.0.0.0", port=port)