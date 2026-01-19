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

# --- 🧠 LOGIC: FILE PARSER ---
def process_rta_files(uploaded_files, passwords):
    all_data = []
    pwd_list = [p.strip() for p in passwords.split(",")] if passwords else [None]

    for uploaded_file in uploaded_files:
        df = None
        success_read = False
        
        try:
            # 1. Handle ZIP / CSV
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

            # 2. Extract Data
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

                        # Allow row if it has EITHER Name OR PAN (We will fix missing PANs later)
                        if t['name'] or t['pan']: all_data.append(t)
                    except: continue

        except Exception as e: st.error(f"Error reading {uploaded_file.name}: {e}")

    return all_data

# --- 💾 LOGIC: SMART SYNC WITH NAME LOOKUP ---
def sync_to_db(data):
    if not data: return 0, ["No data found"]
    
    # 1. FETCH KNOWN CLIENTS (Build the Brain)
    # We fetch all existing clients to map Name -> PAN
    try:
        db_clients = supabase.table('clients').select('name, pan').execute()
        # Create a dictionary: {'JOHN DOE': 'ABCDE1234F', ...}
        name_map = {item['name'].strip().upper(): item['pan'] for item in db_clients.data if item['name'] and item['pan']}
    except Exception as e:
        name_map = {} # Fallback if DB fetch fails
        print(f"Lookup Warning: {e}")

    df = pd.DataFrame(data)
    errors = []
    
    # 2. FILL MISSING PANS
    # If PAN is missing but Name matches our DB, fill it in!
    def fill_pan(row):
        if not row['pan'] and row['name']:
            clean_name = str(row['name']).strip().upper()
            return name_map.get(clean_name, None) # Returns PAN if found, else None
        return row['pan']

    df['pan'] = df.apply(fill_pan, axis=1)

    # 3. SYNC NEW CLIENTS (Only if PAN exists)
    unique_clients = df[['pan', 'name', 'email', 'phone']].drop_duplicates(subset=['pan'])
    for _, row in unique_clients.iterrows():
        if row['pan'] and row['name']:
            payload = {k: v for k, v in row.to_dict().items() if v and k in ['pan', 'name', 'email', 'phone']}
            try:
                supabase.table('clients').upsert(payload).execute()
            except Exception as e:
                errors.append(f"Client Error {row['pan']}: {e}")

    # 4. SYNC STAGING (Log All)
    try:
        staging_data = df[['pan', 'name', 'email', 'phone', 'scheme', 'units', 'action']].where(pd.notnull(df), None).to_dict('records')
        for i in range(0, len(staging_data), 100):
            supabase.table('staging_clients').insert(staging_data[i:i+100]).execute()
    except Exception as e:
        errors.append(f"Staging Error: {e}")

    # 5. SYNC PORTFOLIO (Only works if we found a PAN)
    portfolio_df = df[df['scheme'].notna() & df['pan'].notna()].copy()
    
    if not portfolio_df.empty:
        portfolio_df['signed_units'] = portfolio_df.apply(
            lambda x: -x['units'] if x['action'] == 'DEDUCT' else x['units'], axis=1
        )
        aggregated = portfolio_df.groupby(['pan', 'scheme'])['signed_units'].sum().reset_index()
        
        progress = st.progress(0)
        total_groups = len(aggregated)
        
        for i, (idx, row) in enumerate(aggregated.iterrows()):
            pan = row['pan']
            scheme = row['scheme']
            net_change = row['signed_units']
            
            try:
                res = supabase.table('portfolio_snapshot').select('total_units')\
                    .eq('pan', pan).eq('scheme_name', scheme).execute()
                current = float(res.data[0]['total_units']) if res.data else 0.0
                
                supabase.table('portfolio_snapshot').upsert({
                    'pan': pan, 'scheme_name': scheme, 'total_units': round(current + net_change, 4)
                }, on_conflict='pan,scheme_name').execute()
            except Exception as e:
                errors.append(f"Portfolio Error {pan}: {e}")
            
            if i % 10 == 0: progress.progress((i + 1) / total_groups)
        progress.empty()

    return len(data), errors

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd_input = st.text_input("Zip Passwords", type="password")

if st.button("🚀 Process & Sync"):
    if files:
        with st.spinner("Processing..."):
            results = process_rta_files(files, pwd_input)
            if results:
                s, err_list = sync_to_db(results)
                st.success(f"✅ Processed {s} transactions!")
                if err_list:
                    with st.expander("⚠️ View Errors"):
                        for e in err_list: st.write(e)
            else:
                st.warning("No data found.")