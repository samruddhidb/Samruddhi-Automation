import os
import requests
import re
from datetime import datetime
from supabase import create_client

# --- DEBUG CONFIG ---
print("🚀 Starting Ultimate NAV Engine...")

# GITHUB SECRETS (or Local for testing)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# LOCAL FALLBACK (If you run this on your laptop locally)
# Uncomment and fill these if running on laptop:
# SUPABASE_URL = "your_url_here"
# SUPABASE_KEY = "your_key_here"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Secrets missing.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🧠 LOGIC FROM YOUR TEXT FILE: GENERATE CLEAN ID
def generate_clean_id(scheme_name):
    # Replaces non-alphanumeric with "_" and uppercases
    # Example: "Axis Bluechip - Direct" -> "AXIS_BLUECHIP___DIRECT"
    return re.sub(r'[^a-zA-Z0-9]', '_', scheme_name).upper()

def run_engine():
    # 1. GET RADAR (Schemes we watch)
    print("📡 Fetching Watchlist...")
    radar = supabase.table('watched_schemes').select('scheme_code').execute()
    radar_codes = {r['scheme_code'] for r in radar.data}

    if not radar_codes:
        print("ℹ️ No schemes to watch.")
        return

    print(f"🌍 Downloading AMFI Data for {len(radar_codes)} schemes...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(AMFI_URL, stream=True, timeout=30, headers=headers)
        
        updates = []
        
        for line in response.iter_lines():
            if line:
                try:
                    decoded = line.decode('utf-8')
                    parts = decoded.split(';')
                    
                    # AMFI FORMAT CHECK
                    # 0: Code, 1: ISIN1, 2: ISIN2, 3: Name, 4: NAV, 5: Date
                    if len(parts) >= 6:
                        code = parts[0]
                        
                        # MATCHING LOGIC
                        if code in radar_codes:
                            isin1 = parts[1]
                            isin2 = parts[2]
                            name = parts[3]
                            nav = float(parts[4])
                            date_str = parts[5].strip()
                            
                            # Parse Date
                            try:
                                nav_date = datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
                            except:
                                continue # Skip invalid dates

                            # Add to batch
                            updates.append({
                                'scheme_code': code,
                                'scheme_name': name,
                                'isin1': isin1,
                                'isin2': isin2,
                                'current_nav': nav,
                                'last_nav_date': nav_date
                            })
                except:
                    continue

        # 2. BATCH UPDATE
        if updates:
            print(f"💾 Updating {len(updates)} schemes...")
            
            for u in updates:
                # Update Radar (Now with ISINs!)
                supabase.table('watched_schemes').update({
                    'current_nav': u['current_nav'],
                    'last_nav_date': u['last_nav_date'],
                    'isin1': u['isin1'],
                    'isin2': u['isin2']
                }).eq('scheme_code', u['scheme_code']).execute()
                
                # Cascade Update Snapshots
                # Formula: Value = Units * New NAV
                snaps = supabase.table('portfolio_snapshot').select('snapshot_id, total_units').eq('scheme_code', u['scheme_code']).execute()
                for s in snaps.data:
                    new_val = float(s['total_units']) * u['current_nav']
                    supabase.table('portfolio_snapshot').update({
                        'current_value': new_val,
                        'nav_date': u['last_nav_date']
                    }).eq('snapshot_id', s['snapshot_id']).execute()
                    
            print("✅ SUCCESS! Database updated.")
        else:
            print("⚠️ No updates found matching your watchlist.")

    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    run_engine()
