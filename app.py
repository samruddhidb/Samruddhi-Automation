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

# --- 🧠 LOGIC: FILE PARSER (Extracts Raw Data) ---
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
                # Normalize headers
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
                        # Parsing Logic...
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

    return all_data

# --- 💾 LOGIC: SMART AGGREGATION & SYNC ---
def sync_to_db(data):
    if not data: return 0, ["No data found"]
    
    # Convert list to DataFrame for smart calculation
    df = pd.DataFrame(data)
    errors = []
    
    # 1. SYNC CLIENTS (Name, Email, Pan) - Row by Row is fine here
    # We filter only unique PANs to save DB calls
    unique_clients = df[['pan', 'name', 'email', 'phone']].drop_duplicates(subset=['pan'])
    
    for _, row in unique_clients.iterrows():
        if row['pan'] and row['name']:
            payload = {k: v for k, v in row.to_dict().items() if v and k in ['pan', 'name', 'email', 'phone']}
            try:
                supabase.table('clients').upsert(payload).execute()
            except Exception as e:
                errors.append(f"Client Sync Error {row['pan']}: {e}")

    # 2. SYNC STAGING (Log everything)
    # Just insert the raw data so we have a record
    try:
        # Prepare data for staging table (ensure columns match DB)
        staging_data = df[['pan', 'name', 'email', 'phone', 'scheme', 'units', 'action']].where(pd.notnull(df), None).to_dict('records')
        # Batch insert is faster
        for i in range(0, len(staging_data), 100):
            batch = staging_data[i:i+100]
            supabase.table('staging_clients').insert(batch).execute()
    except Exception as e:
        errors.append(f"Staging Error: {e}")

    # 3. SYNC PORTFOLIO (The Hard Math)
    # Filter for rows that have scheme info
    portfolio_df = df[df['scheme'].notna() & df['pan'].notna()].copy()
    
    if not portfolio_df.empty:
        # A. Calculate Net Movement for this batch in Python
        # If Action is DEDUCT, make units negative
        portfolio_df['signed_units'] = portfolio_df.apply(
            lambda x: -x['units'] if x['action'] == 'DEDUCT' else x['units'], axis=1
        )
        
        # B. Group by PAN + SCHEME and Sum them up
        # This handles "multiple transactions for same name+pan combination at once"
        aggregated = portfolio_df.groupby(['pan', 'scheme'])['signed_units'].sum().reset_index()
        
        # C. Update DB
        progress = st.progress(0)
        total_groups = len(aggregated)
        
        for i, (idx, row) in enumerate(aggregated.iterrows()):
            pan = row['pan']
            scheme = row['scheme']
            net_change = row['signed_units']
            
            try:
                # Fetch current balance
                res = supabase.table('portfolio_snapshot').select('total_units')\
                    .eq('pan', pan).eq('scheme_name', scheme).execute()
                
                current_db_units = float(res.data[0]['total_units']) if res.data else 0.0
                
                # Calculate final
                final_units = current_db_units + net_change
                
                # Upsert
                supabase.table('portfolio_snapshot').upsert({
                    'pan': pan,
                    'scheme_name': scheme,
                    'total_units': round(final_units, 4)
                }, on_conflict='pan,scheme_name').execute()
                
            except Exception as e:
                errors.append(f"Portfolio Error {pan} - {scheme}: {e}")
            
            if i % 5 == 0: progress.progress((i + 1) / total_groups)
        
        progress.empty()

    return len(data), errors

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd_input = st.text_input("Zip Passwords", type="password")

if st.button("🚀 Process & Sync"):
    if files:
        with st.spinner("Processing & Calculating..."):
            results = process_rta_files(files, pwd_input)
            
            if not results:
                st.warning("No valid data found in files.")
            else:
                s, err_list = sync_to_db(results)
                st.success(f"✅ Processed {s} transactions!")
                
                if err_list:
                    with st.expander("⚠️ View Errors"):
                        for e in err_list: st.write(e)