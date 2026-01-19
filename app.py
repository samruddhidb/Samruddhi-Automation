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

# --- 🧠 LOGIC: ROBUST PARSER WITH MULTI-PASSWORD ---
def process_rta_files(uploaded_files, passwords):
    all_data = []
    
    # Ensure passwords is a list
    pwd_list = [p.strip() for p in passwords.split(",")] if passwords else [None]

    for uploaded_file in uploaded_files:
        df = None
        success_read = False
        
        try:
            # 1. Handle ZIP Files with Multi-Password Try
            if uploaded_file.name.endswith('.zip'):
                # Try every password in the list
                for pwd in pwd_list:
                    try:
                        with pyzipper.AESZipFile(uploaded_file) as z:
                            if pwd: z.setpassword(pwd.encode('utf-8'))
                            target = next((f for f in z.namelist() if f.lower().endswith('.csv')), None)
                            if not target: break
                            with z.open(target) as f:
                                df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'), on_bad_lines='skip', low_memory=False)
                                success_read = True
                                break # Stop trying passwords if one works
                    except RuntimeError:
                        continue # Wrong password, try next
                
                if not success_read:
                    st.warning(f"🔒 Could not open {uploaded_file.name}. Check passwords.")
                    continue

            # 2. Handle CSV Files
            else:
                df = pd.read_csv(uploaded_file, on_bad_lines='skip', low_memory=False)

            # --- PARSING LOGIC ---
            if df is not None:
                # Clean headers
                df.columns = [str(c).strip().replace("'", "").replace('"', "") for c in df.columns]
                
                fname = uploaded_file.name.upper()
                is_r9 = "R9" in fname
                is_r33 = "R33" in fname
                is_kfin_m = fname.startswith("MFSD211")
                is_kfin_t = fname.startswith("MFSD201")

                for idx, row in df.iterrows():
                    # Default dictionary with all possible fields
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
                            if any(x in nature for x in ['PURCHASE', 'SYSTEMATIC', 'SWITCH IN']): t['action'] = 'ADD'
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

                        if t['name'] or t['pan']: 
                            all_data.append(t)
                            
                    except Exception:
                        continue

        except Exception as e:
            st.error(f"⚠️ Error reading {uploaded_file.name}: {e}")

    return all_data

# --- 💾 LOGIC: SYNC WITH STRICT COLUMN FILTERING ---
def sync_to_db(data):
    success_count = 0
    errors = []
    
    status_bar = st.progress(0)
    total = len(data)

    for i, item in enumerate(data):
        try:
            # FIX: Create a strict payload for Clients Table (Name/Pan/Email/Phone ONLY)
            client_payload = {
                'pan': item.get('pan'),
                'name': item.get('name'),
                'email': item.get('email'),
                'phone': item.get('phone')
            }
            # Remove empty keys so we don't overwrite DB data with None
            client_payload = {k: v for k, v in client_payload.items() if v is not None}

            # 1. Update Clients Tables
            if item.get('pan') and item.get('name'):
                supabase.table('clients').upsert(client_payload).execute()
            elif item.get('pan') or item.get('name'):
                # This fixes the "Could not find column" error by only sending client_payload
                supabase.table('staging_clients').insert(client_payload).execute()

            # 2. Update Portfolio Snapshot (Only if we have Scheme + Units)
            if item.get('scheme') and item.get('units', 0) > 0 and item.get('pan'):
                res = supabase.table('portfolio_snapshot').select('total_units')\
                    .eq('pan', item['pan']).eq('scheme_name', item['scheme']).execute()
                
                old_units = float(res.data[0]['total_units']) if res.data else 0.0
                current_units = item['units']
                
                if item.get('action') == 'ADD':
                    new_units = old_units + current_units
                elif item.get('action') == 'DEDUCT':
                    new_units = old_units - current_units
                else:
                    new_units = old_units # No action, assume snapshot?

                supabase.table('portfolio_snapshot').upsert({
                    'pan': item['pan'], 
                    'scheme_name': item['scheme'], 
                    'total_units': round(new_units, 4)
                }, on_conflict='pan,scheme_name').execute()
            
            success_count += 1
            
        except Exception as e:
            # Capture the exact error for the user
            errors.append(f"Row {item.get('source_row')}: {str(e)}")
        
        if i % 10 == 0: status_bar.progress((i + 1) / total)
    
    status_bar.empty()
    return success_count, errors

# --- 🖥️ UI ---
st.write("### 📤 Upload RTA Zip/CSV Files")
files = st.file_uploader("Select multiple files", type=['zip', 'csv'], accept_multiple_files=True)
pwd_input = st.text_input("Zip Passwords (separated by comma)", type="password", help="Enter all possible passwords: e.g. PAN123, PASS456")

if st.button("🚀 Process & Sync"):
    if files:
        with st.spinner("Processing files..."):
            results = process_rta_files(files, pwd_input)
            st.write(f"📂 Extracted {len(results)} valid rows. Syncing...")
            
            s, err_list = sync_to_db(results)
            
            if s > 0: st.success(f"✅ Successfully synced {s} records!")
            
            if len(err_list) > 0:
                with st.expander(f"⚠️ {len(err_list)} records failed (Click to view details)"):
                    for err in err_list[:20]: # Show first 20 errors
                        st.write(err)
                    if len(err_list) > 20: st.write(f"...and {len(err_list)-20} more.")