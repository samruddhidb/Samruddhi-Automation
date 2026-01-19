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
    """Converts value to a clean string, handling NaNs."""
    if pd.isna(val) or val == 'nan': return None
    s = str(val).replace("'", "").replace('"', "").strip()
    return s if s else None

def clean_float(val):
    """Safely converts to float, defaults to 0.0 if invalid."""
    try:
        if pd.isna(val): return 0.0
        return float(val)
    except:
        return 0.0

# --- 🧠 LOGIC: THE ROBUST PARSER ---
def process_rta_files(uploaded_files, password):
    all_data = []
    for uploaded_file in uploaded_files:
        try:
            # 1. Open the file (Zip or CSV)
            if uploaded_file.name.endswith('.zip'):
                with pyzipper.AESZipFile(uploaded_file) as z:
                    if password: z.setpassword(password.encode('utf-8'))
                    target = next((f for f in z.namelist() if f.lower().endswith('.csv')), None)
                    if not target: continue
                    with z.open(target) as f:
                        # FIX 1: specific settings for messy CAMS files
                        df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'), on_bad_lines='skip', low_memory=False)
            else:
                df = pd.read_csv(uploaded_file, on_bad_lines='skip', low_memory=False)
            
            # Clean column names
            df.columns = [str(c).strip().replace("'", "").replace('"', "") for c in df.columns]
            
            # Detect File Type
            fname = uploaded_file.name.upper()
            is_r9 = "R9" in fname
            is_r33 = "R33" in fname
            is_kfin_m = fname.startswith("MFSD211")
            is_kfin_t = fname.startswith("MFSD201")

            for _, row in df.iterrows():
                # Default empty structure
                t = {'pan': None, 'name': None, 'email': None, 'phone': None, 
                     'scheme': None, 'units': 0.0, 'action': None}
                
                try:
                    # --- CAMS R9 (Identity) ---
                    if is_r9:
                        row_text = " ".join([str(v) for v in row.values])
                        pan_match = re.search(r"([A-Z]{5}[0-9]{4}[A-Z]{1})", row_text)
                        email_match = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", row_text)
                        
                        t['name'] = clean_str(row.get('INV_NAME'))
                        if pan_match: t['pan'] = pan_match.group(1)
                        if email_match: t['email'] = email_match.group(1)
                    
                    # --- CAMS R33 (Transactions) ---
                    elif is_r33:
                        t['scheme'] = clean_str(row.get('SCHEME'))
                        t['name'] = clean_str(row.get('INVNAME'))
                        t['units'] = clean_float(row.get('UNITS'))
                        nature = clean_str(row.get('TRXN_TYPE_FLAG') or "").upper()
                        
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
                        desc = clean_str(row.get('Transaction Description') or "").lower()
                        if "pledging" in desc or "rej." in desc: continue 
                        
                        t['name'] = clean_str(row.get('Investor Name'))
                        t['scheme'] = clean_str(row.get('Fund Description'))
                        t['units'] = clean_float(row.get('Units'))
                        nature = desc.upper()
                        
                        if any(x in nature for x in ['PURCHASE', 'S T P IN', 'SWITCH IN']): t['action'] = 'ADD'
                        elif any(x in nature for x in ['LATERAL SHIFT OUT', 'REDEMPTION', 'SWITCH OUT']): t['action'] = 'DEDUCT'

                    # Only add valid rows
                    if t['name'] or t['pan']: 
                        all_data.append(t)

                except Exception as row_err:
                    # Skip just this row if it fails, don't crash the whole file
                    continue 

        except Exception as e:
            st.warning(f"⚠️ Error processing {uploaded_file.name}: {e}")
            
    return all_data

# --- 💾 LOGIC: SAFE DB UPDATER ---
def sync_to_db(data):
    success_count = 0
    error_count = 0
    
    status_bar = st.progress(0)
    total = len(data)

    for i, item in enumerate(data):
        try:
            # FIX 2: Sanitize dictionary before sending (Remove None values for cleaner inserts)
            clean_item = {k: v for k, v in item.items() if v is not None}

            # 1. Update Clients Table
            if item.get('pan') and item.get('name'):
                # Prepare client payload
                client_data = {
                    'pan': item['pan'],
                    'name': item['name'],
                }
                if item.get('email'): client_data['email'] = item['email']
                if item.get('phone'): client_data['phone'] = item['phone']
                
                supabase.table('clients').upsert(client_data).execute()
            
            # 2. Or Staging
            elif item.get('pan') or item.get('name'):
                supabase.table('staging_clients').insert(clean_item).execute()

            # 3. Update Portfolio Snapshot
            if item.get('scheme') and item.get('units', 0) > 0 and item.get('pan'):
                res = supabase.table('portfolio_snapshot').select('total_units')\
                    .eq('pan', item['pan']).eq('scheme_name', item['scheme']).execute()
                
                old_units = float(res.data[0]['total_units']) if res.data else 0.0
                current_units = float(item['units'])
                
                if item.get('action') == 'ADD':
                    new_units = old_units + current_units
                elif item.get('action') == 'DEDUCT':
                    new_units = old_units - current_units
                else:
                    new_units = old_units # No action defined

                supabase.table('portfolio_snapshot').upsert({
                    'pan': item['pan'], 
                    'scheme_name': item['scheme'],
                    'total_units': round(new_units, 4)
                }, on_conflict='pan,scheme_name').execute()
            
            success_count += 1
            
        except Exception as e:
            # Log the specific error to the console (Viewable in 'Manage App')
            print(f"FAILED ROW: {item} | REASON: {e}")
            error_count += 1
        
        # Update progress bar
        if i % 10 == 0: status_bar.progress((i + 1) / total)
    
    status_bar.empty()
    return success_count, error_count

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd = st.text_input("Zip Password", type="password")

if st.button("🚀 Process & Sync"):
    if files:
        with st.spinner("Processing files..."):
            results = process_rta_files(files, pwd)
            st.write(f"📂 Extracted {len(results)} rows. Syncing to DB...")
            
            s, e = sync_to_db(results)
            
            if s > 0: st.success(f"✅ Successfully synced {s} records!")
            if e > 0: st.error(f"⚠️ {e} records failed to sync. Check logs.")
            if s > 0: st.balloons()