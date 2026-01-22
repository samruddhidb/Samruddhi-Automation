import os
import requests
import toml
import pathlib
from supabase import create_client
from datetime import datetime

# --- 1. SMART CREDENTIAL LOADER ---
# Priority A: Check Cloud Environment (GitHub Actions / Streamlit Cloud)
SUPABASE_URL = os.environ.get("https://lzkmnkwomccqsclvvqwp.supabase.co")
SUPABASE_KEY = os.environ.get("sb_secret_Pv7eZ34CislDThQvu_sF-A_r1ZeNAWD")

# Priority B: Check Local File (Your Laptop)
if not SUPABASE_URL:
    try:
        # Find the .streamlit/secrets.toml file relative to this script
        script_dir = pathlib.Path(__file__).parent.absolute()
        secrets_path = script_dir / ".streamlit" / "secrets.toml"
        
        if secrets_path.exists():
            print(f"🔍 Found local secrets at: {secrets_path}")
            data = toml.load(secrets_path)
            SUPABASE_URL = data["supabase"]["url"]
            SUPABASE_KEY = data["supabase"]["key"]
            print("✅ Loaded Cloud credentials from local file.")
    except Exception as e:
        print(f"⚠️ Could not read local secrets: {e}")

# Check if we found them
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ FATAL ERROR: No API Keys found!")
    print("   - Local: Check .streamlit/secrets.toml")
    print("   - Deployed: Check GitHub Repository Secrets")
    exit(1)

# --- 2. THE NAV ENGINE ---
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("🚀 Starting NAV Engine...")
try:
    # Fetch schemes specifically from the 'watched_schemes' table
    response = supabase.table('watched_schemes').select('scheme_name').execute()
    schemes = [item['scheme_name'] for item in response.data]
except Exception as e:
    print(f"❌ Database Connect Error: {e}")
    exit(1)

if not schemes:
    print("⚠️ No schemes found in 'watched_schemes' table.")
    exit()

print(f"🔄 Updating {len(schemes)} schemes...")
updated_count = 0

for scheme in schemes:
    try:
        # 1. Search MFAPI for the Scheme Code
        search_url = f"https://api.mfapi.in/mf/search?q={scheme}"
        search_res = requests.get(search_url).json()
        
        if search_res:
            code = search_res[0]['schemeCode']
            
            # 2. Get Latest NAV
            nav_url = f"https://api.mfapi.in/mf/{code}"
            nav_data = requests.get(nav_url).json()
            
            if nav_data.get('data'):
                current_nav = float(nav_data['data'][0]['nav'])
                
                # 3. Save to DB
                supabase.table('watched_schemes').upsert({
                    'scheme_name': scheme,
                    'nav': current_nav,
                    'updated_at': datetime.now().isoformat()
                }).execute()
                
                print(f"✅ {scheme}: {current_nav}")
                updated_count += 1
            else:
                print(f"⚠️ No data found for {scheme}")
        else:
            print(f"⚠️ Could not find code for {scheme}")
            
    except Exception as e:
        print(f"❌ Error updating {scheme}: {e}")

print(f"🎉 Done! Updated {updated_count}/{len(schemes)} schemes.")