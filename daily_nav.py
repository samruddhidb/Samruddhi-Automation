import os
import requests
from datetime import datetime
from supabase import create_client

# --- DEBUG CONFIG ---
print("🚀 Starting Debug Mode...")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# 1. CHECK SECRETS
if not SUPABASE_URL:
    print("❌ ERROR: SUPABASE_URL is missing from GitHub Secrets.")
    exit(1)
if not SUPABASE_KEY:
    print("❌ ERROR: SUPABASE_KEY is missing from GitHub Secrets.")
    exit(1)

print("✅ Secrets found. Connecting to Supabase...")

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ ERROR: Could not create Supabase client. {e}")
    exit(1)

def run_debug_scan():
    # 2. CHECK DATABASE CONNECTION
    print("📡 Fetching 'watched_schemes' from database...")
    try:
        radar = supabase.table('watched_schemes').select('scheme_code').execute()
        radar_codes = {r['scheme_code'] for r in radar.data}
        print(f"✅ Database connected. Found {len(radar_codes)} schemes to watch.")
    except Exception as e:
        print(f"❌ ERROR: Database connection failed. Check your API Keys. Details: {e}")
        return

    if not radar_codes:
        print("ℹ️ No schemes found in 'watched_schemes'. Nothing to do.")
        return

    # 3. CHECK AMFI DOWNLOAD
    print("🌍 Downloading AMFI NAV file...")
    try:
        # Added User-Agent because some sites block python scripts
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(AMFI_URL, stream=True, timeout=10, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ ERROR: AMFI site returned error code {response.status_code}")
            return
            
        print("✅ Connection established. Scanning file lines...")
        
        updates = []
        line_count = 0
        
        for line in response.iter_lines():
            line_count += 1
            if line:
                try:
                    decoded = line.decode('utf-8')
                    parts = decoded.split(';')
                    # Check format
                    if len(parts) >= 6:
                        code = parts[0]
                        if code in radar_codes:
                            nav = float(parts[4])
                            date_str = parts[5]
                            # Clean Date
                            nav_date = datetime.strptime(date_str.strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
                            
                            updates.append({
                                'scheme_code': code,
                                'current_nav': nav,
                                'last_nav_date': nav_date
                            })
                except Exception as parse_err:
                    # Ignore minor parse errors on header lines, but print first one
                    if line_count < 5: 
                        print(f"⚠️ skipped line {line_count}: {parse_err}")
                    continue

        print(f"✅ Scanned {line_count} lines. Found {len(updates)} updates.")

        # 4. PERFORM UPDATES
        if updates:
            print("💾 Writing updates to database...")
            for u in updates:
                # Update Radar
                supabase.table('watched_schemes').update(u).eq('scheme_code', u['scheme_code']).execute()
                
                # Update Snapshots
                snaps = supabase.table('portfolio_snapshot').select('snapshot_id, total_units').eq('scheme_code', u['scheme_code']).execute()
                for s in snaps.data:
                    new_val = float(s['total_units']) * u['current_nav']
                    supabase.table('portfolio_snapshot').update({
                        'current_value': new_val,
                        'nav_date': u['last_nav_date']
                    }).eq('snapshot_id', s['snapshot_id']).execute()
            print("🎉 SUCCESS! Database updated.")
        else:
            print("⚠️ Parsed file but found no matching schemes to update.")

    except Exception as e:
        print(f"❌ ERROR during AMFI download/parse: {e}")

if __name__ == "__main__":
    run_debug_scan()
