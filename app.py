import streamlit as st
import pandas as pd
import requests
import io
import pyzipper
import re
from datetime import datetime
from supabase import create_client

# --- ⚙️ CONFIGURATION ---
AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# Must be the first Streamlit command
st.set_page_config(page_title="Samruddhi Portfolio Master", layout="wide")

# --- 🔐 DATABASE CONNECTION ---
try:
    supabase = create_client(st.secrets["supabase"]["url"], st.secrets["supabase"]["key"])
    st.sidebar.success("✅ Database Connected")
except Exception as e:
    st.error(f"❌ Database Error: {e}")
    st.stop()

# --- 🛠️ HELPERS ---
def parse_date(date_str):
    if pd.isna(date_str): return None
    clean = str(date_str).strip().replace("'", "").replace('"', "")
    for fmt in ["%d-%b-%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]:
        try: return datetime.strptime(clean, fmt).date()
        except: continue
    return None

def clean_str(val):
    if pd.isna(val): return ""
    return str(val).replace("'", "").replace('"', "").strip()

# --- 🧠 LOGIC: THE UNIVERSAL PARSER ---
def process_rta_files(uploaded_files, password):
    all_data = []
    for uploaded_file in uploaded_files:
        try:
            if uploaded_file.name.endswith('.zip'):
                with pyzipper.AESZipFile(uploaded_file) as z:
                    if password: z.setpassword(password.encode('utf-8'))
                    target = next((f for f in z.namelist() if f.lower().endswith('.csv')), None)
                    if not target: continue
                    with z.open(target) as f:
                        df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'))
            else:
                df = pd.read_csv(uploaded_file)
            
            df.columns = [clean_str(c) for c in df.columns]
            
            is_r9 = "R9" in uploaded_file.name.upper()
            is_r33 = "R33" in uploaded_file.name.upper()
            is_kfin_m = uploaded_file.name.startswith("MFSD211")
            is_kfin_t = uploaded_file.name.startswith("MFSD201")

            for _, row in df.iterrows():
                t = {'pan': None, 'name': None, 'email': None, 'phone': None, 'scheme': None, 'units': 0.0, 'action': None}
                
                # --- CAMS R9: ROW-WISE SCAN ---
                if is_r9:
                    row_text = " ".join([str(v) for v in row.values])
                    pan_match = re.search(r"'([A-Z]{5}[0-9]{4}[A-Z]{1})'", row_text)
                    email_match = re.search(r"'([^']+@mail\.com[^']*)'", row_text)
                    t['name'] = clean_str(row.get('INV_NAME'))
                    if pan_match: t['pan'] = pan_match.group(1)
                    if email_match: t['email'] = email_match.group(1)
                
                # --- CAMS R33: TRANSACTIONS ---
                elif is_r33:
                    t['scheme'] = clean_str(row.get('SCHEME'))
                    t['name'] = clean_str(row.get('INVNAME'))
                    t['units'] = float(row.get('UNITS', 0))
                    nature = clean_str(row.get('TRXN_TYPE_FLAG')).upper()
                    if any(x in nature for x in ['PURCHASE', 'SYSTEMATIC INSTALLMENT', 'SWITCH IN']): t['action'] = 'ADD'
                    elif any(x in nature for x in ['REDEMPTION', 'TRANSFER', 'WITHDRAWAL', 'SWITCH OUT']): t['action'] = 'DEDUCT'
                
                # --- KFIN MASTER ---
                elif is_kfin_m:
                    t['name'] = clean_str(row.get('Investor Name'))
                    t['email'] = clean_str(row.get('Email ID') or row.get('Investor Name'))
                    t['pan'] = clean_str(row.get('PAN Number'))
                    t['phone'] = clean_str(row.get('Mobile Number'))
                
                # --- KFIN TRANSACTION ---
                elif is_kfin_t:
                    desc = clean_str(row.get('Transaction Description')).lower()
                    # Filter pledging - ERROR FIXED HERE
                    if "pledging" in desc or "rej." in desc: continue 
                    t['name'] = clean_str(row.get('Investor Name'))
                    t['scheme'] = clean_str(row.get('Fund Description'))
                    t['units'] = float(row.get('Units', 0))
                    nature = desc.upper()
                    if any(x in nature for x in ['PURCHASE', 'S T P IN', 'SWITCH IN']): t['action'] = 'ADD'
                    elif any(x in nature for x in ['LATERAL SHIFT OUT', 'REDEMPTION', 'SWITCH OUT']): t['action'] = 'DEDUCT'

                if t['name'] or t['pan']: all_data.append(t)
        except Exception as e:
            st.warning(f"⚠️ Error processing {uploaded_file.name}: {e}")
    return all_data

# --- 💾 LOGIC: DIRECT SNAPSHOT UPDATER ---
def sync_to_db(data):
    for item in data:
        # Smart Identity Linking
        if item['pan'] and item['name']:
            supabase.table('clients').upsert({
                'pan': item['pan'], 'name': item['name'],
                'email': item['email'], 'phone': item['phone']
            }).execute()
        elif item['pan'] or item['name']:
            supabase.table('staging_clients').insert(item).execute()
            continue

        # Balance Logic (Directly in Snapshot)
        if item['scheme'] and item['units'] > 0:
            res = supabase.table('portfolio_snapshot').select('total_units')\
                .eq('pan', item['pan']).eq('scheme_name', item['scheme']).execute()
            
            old_units = float(res.data[0]['total_units']) if res.data else 0.0
            new_units = old_units + item['units'] if item['action'] == 'ADD' else old_units - item['units']
            
            supabase.table('portfolio_snapshot').upsert({
                'pan': item['pan'], 'scheme_name': item['scheme'],
                'total_units': round(new_units, 4) # 4 decimal support 
            }, on_conflict='pan,scheme_name').execute()

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd = st.text_input("Zip Password", type="password")

if st.button("🚀 Process & Sync"):
    if files:
        results = process_rta_files(files, pwd)
        sync_to_db(results)
        st.success("✅ Data Parsed and Snapshots Updated.")
        st.balloons()