import streamlit as st
import pandas as pd
import io
import pyzipper
import re
from datetime import datetime
from supabase import create_client

# --- ⚙️ CONFIGURATION ---
st.set_page_config(page_title="Samruddhi Portfolio Master", layout="wide")

# --- 🔐 DATABASE CONNECTION ---
try:
    supabase = create_client(st.secrets["supabase"]["url"], st.secrets["supabase"]["key"])
    st.sidebar.success("✅ Database Connected")
except Exception as e:
    st.error(f"❌ Database Error: {e}")
    st.stop()

# --- 🛠️ HELPERS ---
def clean_str(val):
    if pd.isna(val) or str(val).lower() == 'nan': return None
    s = str(val).replace("'", "").replace('"', "").strip()
    return s if s else None

def clean_float(val):
    try:
        if pd.isna(val): return 0.0
        return float(val)
    except:
        return 0.0

def fetch_latest_navs(schemes):
    nav_map = {}
    try:
        response = supabase.table('watched_schemes').select('scheme_name, nav').execute()
        for item in response.data:
            nav_map[item['scheme_name']] = float(item['nav'])
    except: pass
    return nav_map

# --- 🧠 LOGIC: FILE PARSER ---
def process_rta_files(uploaded_files, passwords):
    all_data = []
    pwd_list = [p.strip() for p in passwords.split(",")] if passwords else [None]
    is_lifetime_reset = False

    for uploaded_file in uploaded_files:
        df = None
        success_read = False
        
        # Check for Lifetime Reset
        if "APR2024" in uploaded_file.name.upper() or "LIFETIME" in uploaded_file.name.upper():
            is_lifetime_reset = True
            st.toast(f"⚠️ LIFETIME RESET DETECTED in {uploaded_file.name}!", icon="🔥")

        try:
            if uploaded_file.name.endswith('.zip'):
                for pwd in pwd_list:
                    try:
                        with pyzipper.AESZipFile(uploaded_file) as z:
                            if pwd: z.setpassword(pwd.encode('utf-8'))
                            target = next((f for f in z.namelist() if f.lower().endswith('.csv')), None)
                            if not target: break
                            with z.open(target) as f:
                                df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'), on_bad_lines='skip', low_memory=False)
                                success_read = True
                                break 
                    except RuntimeError: continue
                if not success_read:
                    st.warning(f"🔒 Could not open {uploaded_file.name}. Check passwords.")
                    continue
            else:
                df = pd.read_csv(uploaded_file, on_bad_lines='skip', low_memory=False)

            if df is not None:
                df.columns = [str(c).strip().replace("'", "").replace('"', "") for c in df.columns]
                fname = uploaded_file.name.upper()
                is_r9 = "R9" in fname or "IDENTITY" in fname
                is_r33 = "R33" in fname or "TRXN" in fname
                is_kfin_m = fname.startswith("MFSD211")
                is_kfin_t = fname.startswith("MFSD201")

                for idx, row in df.iterrows():
                    t = {'pan': None, 'name': None, 'email': None, 'phone': None, 
                         'scheme': None, 'units': 0.0, 'action': None, 'source_row': idx + 2}
                    
                    try:
                        if is_r9:
                            row_text = " ".join([str(v) for v in row.values])
                            pan_match = re.search(r"([A-Z]{5}[0-9]{4}[A-Z]{1})", row_text)
                            email_match = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", row_text)
                            t['name'] = clean_str(row.get('INV_NAME'))
                            if pan_match: t['pan'] = pan_match.group(1)
                            if email_match: t['email'] = email_match.group(1)

                        elif is_r33:
                            t['scheme'] = clean_str(row.get('SCHEME'))
                            t['name'] = clean_str(row.get('INVNAME'))
                            t['units'] = clean_float(row.get('UNITS'))
                            nature = clean_str(row.get('TRXN_TYPE_FLAG') or "").upper()
                            if any(x in nature for x in ['PURCHASE', 'SYSTEMATIC', 'SWITCH IN', 'REINVESTMENT']): t['action'] = 'ADD'
                            elif any(x in nature for x in ['REDEMPTION', 'TRANSFER', 'SWITCH OUT']): t['action'] = 'DEDUCT'

                        elif is_kfin_m:
                            t['name'] = clean_str(row.get('Investor Name'))
                            t['email'] = clean_str(row.get('Email ID'))
                            t['pan'] = clean_str(row.get('PAN Number'))
                            t['phone'] = clean_str(row.get('Mobile Number'))

                        elif is_kfin_t:
                            desc = clean_str(row.get('Transaction Description') or "").lower()
                            if "pledging" in desc or "rej." in desc: continue 
                            t['name'] = clean_str(row.get('Investor Name'))
                            t['scheme'] = clean_str(row.get('Fund Description'))
                            t['units'] = clean_float(row.get('Units'))
                            nature = desc.upper()
                            if any(x in nature for x in ['PURCHASE', 'S T P IN', 'SWITCH IN']): t['action'] = 'ADD'
                            elif any(x in nature for x in ['SHIFT OUT', 'REDEMPTION', 'SWITCH OUT']): t['action'] = 'DEDUCT'

                        if t['name'] or t['pan']: all_data.append(t)
                    except: continue

        except Exception as e: st.error(f"Error reading {uploaded_file.name}: {e}")

    return all_data, is_lifetime_reset

# --- 💾 LOGIC: TWO-PASS INTELLIGENT SYNC ---
def sync_to_db(data, is_reset_mode):
    if not data: return 0, ["No data found"]
    
    errors = []
    
    # --- PASS 1: BUILD THE BRAIN (Identify Everyone First) ---
    # Fetch existing DB clients
    try:
        db_clients = supabase.table('clients').select('name, pan').execute()
        # Start with what the DB knows
        name_map = {item['name'].strip().upper(): item['pan'] for item in db_clients.data if item['name'] and item['pan']}
    except: name_map = {}

    # Update map with NEW people found in this file (The Fix!)
    for row in data:
        if row['pan'] and row['name']:
            clean_name = str(row['name']).strip().upper()
            name_map[clean_name] = row['pan'] # Add new guy to memory immediately

    # --- PASS 2: PROCESS TRANSACTIONS (Using the Updated Brain) ---
    df = pd.DataFrame(data)

    # 1. Fill Missing PANs using the NOW COMPLETE name_map
    def fill_pan(row):
        if not row['pan'] and row['name']:
            return name_map.get(str(row['name']).strip().upper(), None)
        return row['pan']
    df['pan'] = df.apply(fill_pan, axis=1)

    # 2. SEPARATE STREAMS: Master Data vs Transaction Data
    # Identify Master Rows (Have PAN/Name but NO Scheme info)
    master_df = df[df['pan'].notna() & df['scheme'].isna()]
    
    # Identify Transaction Rows (Have Scheme info)
    transaction_df = df[df['scheme'].notna()]

    # 3. FAIL-SAFE RESET
    if is_reset_mode:
        unique_reset_pans = df[df['pan'].notna()]['pan'].unique()
        if len(unique_reset_pans) > 0:
            try:
                supabase.table('portfolio_snapshot').delete().in_('pan', list(unique_reset_pans)).execute()
                errors.append(f"⚠️ RESET TRIGGERED: Cleared portfolio for {len(unique_reset_pans)} clients.")
            except Exception as e: errors.append(f"Reset Failed: {e}")

    # 4. SYNC CLIENTS (Only from Master Rows + valid Transaction rows)
    # We grab unique clients from EVERYWHERE to ensure no one is missed
    unique_clients = df[['pan', 'name', 'email', 'phone']].drop_duplicates(subset=['pan'])
    for _, row in unique_clients.iterrows():
        if row['pan'] and row['name']:
            payload = {k: v for k, v in row.to_dict().items() if v and k in ['pan', 'name', 'email', 'phone']}
            try: supabase.table('clients').upsert(payload).execute()
            except: pass

    # 5. SYNC STAGING (The "Raw Log")
    # We still log everything here for safety, but now the PANs are filled!
    try:
        staging_data = df[['pan', 'name', 'email', 'phone', 'scheme', 'units', 'action']].where(pd.notnull(df), None).to_dict('records')
        for i in range(0, len(staging_data), 100):
            supabase.table('staging_clients').insert(staging_data[i:i+100]).execute()
    except Exception as e: errors.append(f"Staging Error: {e}")

    # 6. SYNC PORTFOLIO (Only from Transaction Rows with valid PANs)
    # Filter: Must have Scheme AND PAN
    valid_transactions = transaction_df[transaction_df['pan'].notna()].copy()
    
    if not valid_transactions.empty:
        # Get NAVs
        nav_map = fetch_latest_navs(set(valid_transactions['scheme'].unique()))

        valid_transactions['signed_units'] = valid_transactions.apply(
            lambda x: -x['units'] if x['action'] == 'DEDUCT' else x['units'], axis=1
        )
        
        aggregated = valid_transactions.groupby(['pan', 'scheme'])['signed_units'].sum().reset_index()
        
        progress = st.progress(0)
        total = len(aggregated)
        
        for i, (idx, row) in enumerate(aggregated.iterrows()):
            pan = row['pan']
            scheme = row['scheme']
            net_change = row['signed_units']
            
            try:
                current_units = 0.0
                if not is_reset_mode:
                    res = supabase.table('portfolio_snapshot').select('total_units')\
                        .eq('pan', pan).eq('scheme_name', scheme).execute()
                    current_units = float(res.data[0]['total_units']) if res.data else 0.0
                
                final_units = current_units + net_change
                nav = nav_map.get(scheme, 0.0)
                current_value = round(final_units * nav, 2)

                supabase.table('portfolio_snapshot').upsert({
                    'pan': pan, 
                    'scheme_name': scheme, 
                    'total_units': round(final_units, 4),
                    'current_value': current_value,
                    'nav': nav,
                    'updated_at': datetime.now().isoformat()
                }, on_conflict='pan,scheme_name').execute()
                
            except Exception as e: errors.append(f"Portfolio Error {pan}: {e}")
            
            if i % 10 == 0: progress.progress((i + 1) / total)
        progress.empty()

    return len(data), errors

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd_input = st.text_input("Zip Passwords", type="password")

if st.button("🚀 Process & Sync"):
    if files:
        with st.spinner("Processing & Calculating..."):
            data, is_reset = process_rta_files(files, pwd_input)
            
            if data:
                s, err_list = sync_to_db(data, is_reset)
                st.success(f"✅ Processed {s} transactions!")
                if is_reset: st.warning("🔄 Lifetime Reset was active for this batch.")
                
                if err_list:
                    with st.expander("⚠️ View Logs"):
                        for e in err_list: st.write(e)
            else:
                st.warning("No data found.")