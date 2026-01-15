import os
import time
import requests
from datetime import datetime
from supabase import create_client

# --- CONFIG (Reads from GitHub Secrets) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Error: Secrets not found.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_navs():
    print("📡 Connecting to Database...")
    # 1. Get List of Schemes we own (The Radar)
    radar = supabase.table('watched_schemes').select('scheme_code').execute()
    radar_codes = {r['scheme_code'] for r in radar.data}

    if not radar_codes:
        print("ℹ️ No schemes to watch.")
        return True # Success (Nothing to do)

    print(f"🔍 Scanning AMFI for {len(radar_codes)} schemes...")
    updates = []
    
    # 2. Download & Parse AMFI (Streamed)
    try:
        response = requests.get(AMFI_URL, stream=True, timeout=30)
        if response.status_code != 200:
            print(f"⚠️ AMFI Site returned {response.status_code}")
            return False
            
        for line in response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                parts = decoded.split(';')
                if len(parts) >= 6 and parts[0] in radar_codes:
                    updates.append({
                        'scheme_code': parts[0],
                        'current_nav': float(parts[4]),
                        'last_nav_date': datetime.strptime(parts[5], "%d-%b-%Y").strftime("%Y-%m-%d")
                    })
    except Exception as e:
        print(f"⚠️ Network Error: {e}")
        return False

    # 3. Batch Update
    if updates:
        print(f"⚡ Updating {len(updates)} schemes...")
        for u in updates:
            # Update Radar
            supabase.table('watched_schemes').update(u).eq('scheme_code', u['scheme_code']).execute()
            
            # Update Snapshots (Calculated Field)
            # Find snapshots with this scheme
            snaps = supabase.table('portfolio_snapshot').select('snapshot_id, total_units').eq('scheme_code', u['scheme_code']).execute()
            for s in snaps.data:
                new_val = float(s['total_units']) * u['current_nav']
                supabase.table('portfolio_snapshot').update({
                    'current_value': new_val,
                    'nav_date': u['last_nav_date']
                }).eq('snapshot_id', s['snapshot_id']).execute()
                
    print("✅ Success!")
    return True

# --- MAIN EXECUTION WITH RETRY ---
if __name__ == "__main__":
    attempt = 1
    max_retries = 2
    
    while attempt <= max_retries:
        print(f"\n🚀 Attempt {attempt} of {max_retries}...")
        success = fetch_navs()
        
        if success:
            print("🎉 Job Finished Successfully.")
            exit(0)
        else:
            if attempt < max_retries:
                print("❌ Failed. Waiting 1 hour (3600 seconds) before retry...")
                time.sleep(3600) # Wait 1 hour as requested
            else:
                print("💀 Failed after final attempt.")
                exit(1) # Mark job as failed in GitHub
        
        attempt += 1