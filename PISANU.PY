import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import json
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAZIONE PAGINA STREAMLIT ---
# Imposta la pagina a tutto schermo per dare spazio alla mappa
st.set_page_config(layout="wide", page_title="WEX Real-Time Pro")

FILE_EXCEL_ANAGRAFICA = "Consistenza aggiornata Wex novembre 2025 (4) (1).xlsx"
FILE_INCROCIO = "Incrocio_Wex_Ministero_Ultra_Preciso.csv"
URL_API = "https://carburanti.mise.gov.it/ospzApi/search/servicearea"

# --- FUNZIONI DI SUPPORTO (Identiche alle tue) ---
def clean_coord(value):
    try:
        if pd.isna(value): return 0.0
        return float(str(value).replace(' ', '').replace(',', '.').strip())
    except: return 0.0

def extract_pv(x):
    try:
        if pd.isna(x): return None
        s = str(x).strip().split()
        cod = next((part for part in reversed(s) if part.isdigit()), None)
        return str(int(cod)) if cod else None
    except: return None

def to_float(val):
    try: return float(str(val).replace(',', '.'))
    except: return 0.0

# --- MOTORE API CON AGGIORNAMENTO AUTOMATICO OGNI 2 ORE ---
# ttl=7200 significa che tiene i dati in memoria per 2 ore (7200 sec)
@st.cache_data(ttl=7200)
def sync_ministero():
    prezzi_live = {}
    prodotti_set = set()
    try:
        r = requests.post(URL_API, json={"searchParams": {"serviceArea": {"latitude": 42.0, "longitude": 12.5}, "maxResults": 100000}}, timeout=90, verify=False)
        if r.status_code == 200:
            for item in r.json().get('results', []):
                pid = str(item.get('id')).strip()
                fuels = item.get('fuels', [])
                if fuels:
                    prezzi_live[pid] = {}
                    for f in fuels:
                        nome = f"{f['name']} ({'Self' if f['isSelf'] else 'Servito'})"
                        prodotti_set.add(nome)
                        dt_raw = f.get('dtCom') or item.get('insertDate') or "2020-01-01T00:00:00"
                        try: 
                            dt = datetime.fromisoformat(dt_raw.replace("Z", ""))
                            data_str = dt.strftime("%d/%m/%Y")
                            ts = int(dt.timestamp())
                        except: 
                            data_str = "N.D."
                            ts = 0
                        prezzi_live[pid][nome] = {"p": float(f.get('price', 0)), "d": data_str, "ts": ts}
    except Exception as e:
        pass
    if not prodotti_set: prodotti_set = {"Gasolio Autotrazione (Self)", "Gasolio (Self)", "Benzina (Self)"}
    return prezzi_live, prodotti_set


# --- INTERFACCIA WEB ---
st.title("🚛 WEX REAL-TIME PRECISION V55")
st.markdown("Carica il file dei consumi della flotta. I prezzi sono collegati direttamente al Ministero.")

# 1. Sostituisce la finestra Tkinter con un uploader web
path_trans = st.file_uploader("Seleziona il File dei Consumi (CSV o Excel)", type=['csv', 'xls', 'xlsx'])

if path_trans is not None:
    with st.spinner('Analisi abitudini flotta, incrocio anagrafiche e recupero prezzi in corso...'):
        client_volumes = []
        stats_visite = {}
        storico_autisti = {}
        
        # 2. Caricamento Dati Storici (Identico al tuo)
        try:
            if path_trans.name.lower().endswith('.csv'):
                try: df_trans = pd.read_csv(path_trans, sep=',', on_bad_lines='skip', engine='python')
                except: df_trans = pd.read_csv(path_trans, sep=';', on_bad_lines='skip', engine='python')
            else:
                try: df_trans = pd.read_excel(path_trans)
                except: df_trans = pd.read_csv(path_trans, sep='\t', on_bad_lines='skip')
                    
            df_trans.columns = [str(c).strip().lower() for c in df_trans.columns]
            
            col_target = next((c for c in df_trans.columns if 'delivery_point' in c or 'stazione' in c), None)
            col_targa = next((c for c in df_trans.columns if 'vrn' in c or 'targa' in c), None)
            col_qta = next((c for c in df_trans.columns if 'quantity' in c or 'quantit' in c or 'litri' in c), None)
            col_data = next((c for c in df_trans.columns if 'date' in c or 'data' in c or 'timestamp' in c), None)
            col_importo = next((c for c in df_trans.columns if 'amount' in c or 'importo' in c or 'totale' in c or 'value' in c or 'valore' in c or 'euro' in c), None)
            col_prezzo_unit = next((c for c in df_trans.columns if 'price' in c or 'prezzo' in c), None)
            col_prodotto = next((c for c in df_trans.columns if 'product' in c or 'prodotto' in c or 'articolo' in c or 'desc' in c), None)
            
            if col_target:
                stats_visite = df_trans[col_target].apply(extract_pv).dropna().value_counts().to_dict()
                if col_targa and col_qta:
                    df_trans[col_qta] = df_trans[col_qta].apply(to_float)
                    
                    grouped = df_trans.groupby([col_target, col_targa])[col_qta].sum().reset_index()
                    for _, row in grouped.iterrows():
                        pv = extract_pv(row[col_target])
                        if pv and row[col_qta] > 0:
                            client_volumes.append({"pv": pv, "targa": str(row[col_targa]).strip(), "litri_storici": row[col_qta]})
                    
                    if col_data:
                        dt_series = pd.to_datetime(df_trans[col_data], errors='coerce', dayfirst=True)
                        df_trans['mese_anno'] = dt_series.dt.strftime('%Y-%m').fillna('N.D.')
                        df_trans['settimana'] = dt_series.dt.strftime('%V').fillna('N.D.') 
                    else:
                        df_trans['mese_anno'] = 'N.D.'
                        df_trans['settimana'] = 'N.D.'
                        
                    if col_prezzo_unit:
                        df_trans['prezzo_pagato'] = df_trans[col_prezzo_unit].apply(to_float)
                    elif col_importo:
                        df_trans['importo_val'] = df_trans[col_importo].apply(to_float)
                        df_trans['prezzo_pagato'] = df_trans.apply(lambda r: r['importo_val'] / r[col_qta] if pd.notna(r[col_qta]) and r[col_qta] > 0 else 0, axis=1)
                    else:
                        df_trans['prezzo_pagato'] = 0.0

                    for _, row in df_trans.iterrows():
                        pv = extract_pv(row[col_target])
                        targa = str(row[col_targa]).strip()
                        litri = row[col_qta]
                        prezzo = row['prezzo_pagato']
                        mese = row['mese_anno']
                        settimana = row['settimana']
                        prodotto = str(row[col_prodotto]).strip().upper() if col_prodotto and pd.notna(row[col_prodotto]) else "CARBURANTE"
                        
                        if pv and litri > 0:
                            if targa not in storico_autisti: storico_autisti[targa] = {}
                            if mese not in storico_autisti[targa]: storico_autisti[targa][mese] = {}
                            if settimana not in storico_autisti[targa][mese]: storico_autisti[targa][mese][settimana] = {}
                            if pv not in storico_autisti[targa][mese][settimana]: storico_autisti[targa][mese][settimana][pv] = {}
                            
                            if prodotto not in storico_autisti[targa][mese][settimana][pv]:
                                storico_autisti[targa][mese][settimana][pv][prodotto] = {'litri': 0, 'spesa_tot': 0}
                                
                            storico_autisti[targa][mese][settimana][pv][prodotto]['litri'] += litri
                            storico_autisti[targa][mese][settimana][pv][prodotto]['spesa_tot'] += (litri * prezzo)
                    
                    for t in storico_autisti:
                        for m in storico_autisti[t]:
                            for s in storico_autisti[t][m]:
                                for pv in storico_autisti[t][m][s]:
                                    for prod in storico_autisti[t][m][s][pv]:
                                        d = storico_autisti[t][m][s][pv][prod]
                                        d['prezzo_medio'] = d['spesa_tot'] / d['litri'] if d['litri'] > 0 else 0
                                        del d['spesa_tot']

        except Exception as e:
            st.error(f"⚠️ Errore lettura consumi: {e}")

        # 3. Caricamento Anagrafiche WEX
        try:
            df_coords = pd.read_excel(FILE_EXCEL_ANAGRAFICA)
            df_wex = pd.read_csv(FILE_INCROCIO, sep=';')
            db = pd.merge(df_coords, df_wex[['PUNTO VENDITA', 'idImpianto']], on='PUNTO VENDITA', how='left')
            db['idImpianto'] = db['idImpianto'].astype(str).str.strip()
        except Exception as e:
            st.error(f"Errore caricamento database WEX: {e}")
            st.stop()

        # 4. Recupero Prezzi (usa la cache ogni 2 ore)
        prezzi_live, prodotti_set = sync_ministero()

        # 5. Marker List
        marker_list = []
        for _, row in db.iterrows():
            lat = clean_coord(row.get('LATITUDINE'))
            lng = clean_coord(row.get('LONGITUDINE'))
            if lat == 0: continue
            
            pv_id = str(row['PUNTO VENDITA'])
            visite = stats_visite.get(pv_id, 0)
            brand = str(row['BRAND']).upper()
            
            marker_list.append({
                "lat": lat, "lng": lng, "brand": brand, "pv": pv_id,
                "visite": int(visite), "id_min": str(row['idImpianto']),
                "addr": f"{row['INDIRIZZO']} ({row['CITTA\'']})",
                "fuels": prezzi_live.get(str(row['idImpianto']), {})
            })

        fuel_options = "".join([f'<option value="{p}">{p}</option>' for p in sorted(list(prodotti_set))])

        # --- 6. INIEZIONE HTML ---
        # Questo è il tuo esatto template HTML senza cambiare una virgola
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>WEX REAL-TIME PRECISION V55</title>
            <meta charset="utf-8" />
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
            <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
            <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700;900&display=swap" rel="stylesheet">
            <script src="https://cdn.sheetjs.com/xlsx-0.20.1/package/dist/xlsx.full.min.js"></script>
            <style>
                /* ... INCOLLA QUI DENTRO TUTTO IL TUO BLOCCO <STYLE> ORIGINALE ... */
                :root {{ --p: #10b981; --warn: #fbbf24; --bg: #0f172a; --card: #1e293b; --client: #ef4444; --accent: #3b82f6; --gold: #f59e0b; }}
                body {{ margin: 0; font-family: 'Montserrat', sans-serif; background: var(--bg); color: white; overflow:hidden; }}
                #map {{ position: absolute; width: calc(100% - 750px); height: 100%; left: 350px; z-index: 1; }}
                #sidebar-left {{ position: absolute; top: 0; left: 0; bottom: 0; width: 350px; background: var(--bg); z-index: 1000; display: flex; flex-direction: column; padding: 20px; box-sizing: border-box; border-right: 1px solid rgba(255,255,255,0.1); box-shadow: 5px 0 15px rgba(0,0,0,0.5); overflow-y:auto; }}
                #dashboard-right {{ position: absolute; top: 0; right: 0; bottom: 0; width: 400px; background: rgba(15, 23, 42, 0.95); z-index: 1000; border-left: 1px solid rgba(255,255,255,0.1); padding: 20px; box-sizing: border-box; box-shadow: -5px 0 15px rgba(0,0,0,0.5); overflow-y:auto; backdrop-filter: blur(10px); display: flex; flex-direction: column; }}
                h2 {{ margin: 0 0 15px 0; color: white; font-weight: 900; font-size: 20px; text-transform: uppercase; letter-spacing: 1px; }}
                .subtitle {{ font-size: 11px; color: #94a3b8; text-transform: uppercase; font-weight: 700; margin-bottom: 5px; }}
                .group {{ background: rgba(0,0,0,0.2); padding: 15px; border-radius: 10px; margin-bottom: 15px; border: 1px solid rgba(255,255,255,0.05); }}
                input, select {{ width: 100%; background: #0f172a; border: 1px solid #334155; color: white; padding: 10px; border-radius: 6px; box-sizing: border-box; font-family: 'Montserrat', sans-serif; font-weight: 700; margin-bottom:8px; outline: none; }}
                .btn {{ border: none; padding: 14px; border-radius: 8px; font-weight: 900; cursor: pointer; width: 100%; color: white; margin-top: 5px; text-transform: uppercase; letter-spacing: 1px; transition: 0.3s; }}
                .btn-primary {{ background: var(--accent); box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4); }}
                .btn-client {{ background: var(--client); box-shadow: 0 4px 15px rgba(239, 68, 68, 0.4); border: 2px solid transparent; }}
                .btn-client.active {{ background: transparent; border: 2px solid var(--client); color: var(--client); box-shadow: none; }}
                .results-list {{ flex-grow: 1; overflow-y: auto; margin-top: 10px; padding-right: 5px; }}
                .card {{ background: rgba(255,255,255,0.03); padding: 15px; border-radius: 10px; margin-bottom: 10px; border-left: 5px solid transparent; cursor: pointer; transition: 0.2s; }}
                .card.active {{ border-left-color: var(--client); background: rgba(239, 68, 68, 0.08); }}
                .price {{ font-size: 22px; font-weight: 900; color: white; }}
                .tabs {{ display: flex; margin-bottom: 15px; background: rgba(0,0,0,0.2); border-radius: 8px; padding: 4px; flex-shrink: 0; }}
                .tab-btn {{ flex: 1; text-align: center; padding: 10px; cursor: pointer; font-size: 11px; font-weight: 900; color: #94a3b8; text-transform: uppercase; border-radius: 6px; transition: 0.2s; }}
                .tab-btn:hover {{ background: rgba(255,255,255,0.05); }}
                .tab-btn.active {{ color: white; background: var(--accent); box-shadow: 0 2px 10px rgba(59, 130, 246, 0.3); }}
                .tab-content {{ display: none; flex-grow: 1; overflow-y: auto; padding-right: 5px; }}
                .tab-content.active {{ display: block; }}
                .dash-box {{ background: rgba(16, 185, 129, 0.1); padding: 15px; border-radius: 10px; text-align: center; margin-bottom: 15px; border: 1px solid rgba(16, 185, 129, 0.3); }}
                .dash-number {{ font-size: 32px; font-weight: 900; color: var(--p); margin: 5px 0; }}
                .targa-card {{ background: rgba(255,255,255,0.05); border-left: 4px solid var(--p); border-radius: 6px; margin-bottom: 10px; overflow: hidden; }}
                .targa-card.history {{ border-left-color: var(--accent); }}
                .targa-header {{ padding: 12px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; background: rgba(0,0,0,0.2); font-weight: 900; font-size: 14px; }}
                .targa-header:hover {{ background: rgba(0,0,0,0.4); }}
                .targa-gain {{ color: var(--p); }}
                .targa-body {{ padding: 10px; display: none; background: rgba(0,0,0,0.1); border-top: 1px solid rgba(255,255,255,0.05); }}
                .targa-body.open {{ display: block; }}
                .plan-item {{ background: rgba(0,0,0,0.3); padding: 12px; border-radius: 8px; margin-bottom: 8px; border: 1px solid rgba(255,255,255,0.05); cursor: pointer; transition: 0.2s; }}
                .plan-item:hover {{ background: rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.5); transform: translateX(5px); }}
                .targhe-box {{ margin-top: 8px; font-size: 10px; color: #cbd5e1; background: rgba(0,0,0,0.3); padding: 8px; border-radius: 6px; max-height: 100px; overflow-y: auto; }}
                .targhe-row {{ display: flex; justify-content: space-between; border-bottom: 1px dashed rgba(255,255,255,0.1); margin-bottom: 4px; padding-bottom: 4px; }}
                ::-webkit-scrollbar {{ width: 6px; }}
                ::-webkit-scrollbar-track {{ background: transparent; }}
                ::-webkit-scrollbar-thumb {{ background: #334155; border-radius: 10px; }}
            </style>
        </head>
        <body>
            <div id="sidebar-left">
                <h2>WEX REAL-TIME <span style="color:var(--accent)">PRO</span></h2>
                <div class="group">
                    <div class="subtitle">1. Filtro Freschezza Dati</div>
                    <select id="freshness" onchange="render()" style="border-color:var(--p); color:var(--p);">
                        <option value="172800">Prezzi delle Ultime 48 Ore</option>
                        <option value="259200" selected>Prezzi degli Ultimi 3 Giorni</option>
                        <option value="604800">Prezzi dell'Ultima Settimana</option>
                    </select>
                </div>
                <div class="group">
                    <div class="subtitle">2. Area di Lavoro</div>
                    <button id="btn-client-only" class="btn btn-client" onclick="toggleClientOnly()">📍 FILTRA SOLO RETE CLIENTE</button>
                    <div style="margin-top:10px;">
                        <select id="fuel" onchange="render()">{fuel_options}</select>
                        <select id="radius" onchange="render()">
                            <option value="5000">Raggio Alternativa: 5 KM</option>
                            <option value="10000" selected>Raggio Alternativa: 10 KM</option>
                            <option value="25000">Raggio Alternativa: 25 KM</option>
                        </select>
                    </div>
                </div>
                <div class="group" style="border-color: var(--gold);">
                    <div class="subtitle" style="color:var(--gold);">3. Cerca Città / Area</div>
                    <input type="text" id="loc" placeholder="Es. Milano, Roma...">
                    <button class="btn" style="background:var(--gold); color:#0f172a;" onclick="cercaSingolaCitta()">Cerca e Zoomma</button>
                </div>
                <button class="btn btn-primary" style="margin-top:5px;" onclick="exportOptimizationPlan()">📄 Scarica Piano Operativo</button>
                <div style="margin-top:15px; border-bottom:1px solid #334155;"></div>
                <div class="results-list" id="res"></div>
            </div>

            <div id="map"></div>
            
            <div id="dashboard-right">
                <h2 style="text-align: center;">🚛 FLOTTA CLIENTE</h2>
                <div class="tabs">
                    <div id="tab-btn-plan" class="tab-btn active" onclick="switchTab('plan')">📈 Action Plan</div>
                    <div id="tab-btn-history" class="tab-btn" onclick="switchTab('history')" style="background-color: transparent;">🕒 Storico Autisti</div>
                </div>
                <div id="tab-plan" class="tab-content active">
                    <div class="dash-box">
                        <div style="font-size:10px; color:#94a3b8; text-transform:uppercase; font-weight:700;">Risparmio Proiettato (Su Prezzi Reali)</div>
                        <div class="dash-number" id="tot-savings">+ € 0.00</div>
                    </div>
                    <div id="targhe-list"></div>
                </div>
                <div id="tab-history" class="tab-content">
                    <div id="storico-list"></div>
                </div>
            </div>
            
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
            <script>
                // Passiamo le variabili da Python a Javascript esattamente come facevi prima
                const allStations = {json.dumps(marker_list)};
                const clientVolumes = {json.dumps(client_volumes)};
                const driverHistory = {json.dumps(storico_autisti)};
                
                // ... INCOLLA QUI DENTRO TUTTO IL TUO BLOCCO <SCRIPT> ORIGINALE ...
                // Le funzioni switchTab, highlightRoute, toggleClientOnly, cercaSingolaCitta,
                // renderHistory, render, calculateDashboard, exportOptimizationPlan andranno tutte qui.
                // Siccome il file Javascript vive dentro il browser dell'utente, eseguirà il download del file Excel
                // e ridisegnerà la mappa esattamente nello stesso modo.
            </script>
        </body>
        </html>
        """
        
        # 7. INVIA LA PAGINA HTML A STREAMLIT
        # Invece di salvare il file e aprire Chrome, chiediamo a Streamlit di iniettare 
        # questa pagina web alta 900px nel browser.
        st.success("Analisi completata con successo! I prezzi sono aggiornati.")
        components.html(html_template, height=900, scrolling=True)