import pandas as pd
import requests
import webbrowser
import os
import json
import urllib3
import tkinter as tk
from tkinter import filedialog
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAZIONE ---
FILE_EXCEL_ANAGRAFICA = "Consistenza aggiornata Wex novembre 2025 (4) (1).xlsx"
FILE_INCROCIO = "Incrocio_Wex_Ministero_Ultra_Preciso.csv"
URL_API = "https://carburanti.mise.gov.it/ospzApi/search/servicearea"

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

def genera_wex_realtime_precision():
    print("--- WEX REAL-TIME PRECISION V55 (Con Riepiloghi e Prodotti) ---")
    
    # 1. Selezione File
    root = tk.Tk(); root.withdraw(); root.attributes("-topmost", True)
    path_trans = filedialog.askopenfilename(title="Seleziona il File dei Consumi", filetypes=[("File Dati", "*.csv *.xls *.xlsx")])
    root.destroy()
    if not path_trans: return

    # 2. Caricamento Dati Storici
    client_volumes = []
    stats_visite = {}
    storico_autisti = {}
    
    print("Analisi abitudini flotta (Mese, Settimana, Prodotti)...")
    try:
        if path_trans.lower().endswith('.csv'):
            try: df_trans = pd.read_csv(path_trans, sep=',', on_bad_lines='skip', engine='python')
            except: df_trans = pd.read_csv(path_trans, sep=';', on_bad_lines='skip', engine='python')
        else:
            try: df_trans = pd.read_excel(path_trans)
            except: df_trans = pd.read_csv(path_trans, sep='\t', on_bad_lines='skip')
                
        df_trans.columns = [str(c).strip().lower() for c in df_trans.columns]
        
        # Identificazione delle colonne
        col_target = next((c for c in df_trans.columns if 'delivery_point' in c or 'stazione' in c), None)
        col_targa = next((c for c in df_trans.columns if 'vrn' in c or 'targa' in c), None)
        col_qta = next((c for c in df_trans.columns if 'quantity' in c or 'quantit' in c or 'litri' in c), None)
        col_data = next((c for c in df_trans.columns if 'date' in c or 'data' in c or 'timestamp' in c), None)
        col_importo = next((c for c in df_trans.columns if 'amount' in c or 'importo' in c or 'totale' in c or 'value' in c or 'valore' in c or 'euro' in c), None)
        col_prezzo_unit = next((c for c in df_trans.columns if 'price' in c or 'prezzo' in c), None)
        
        # Nuova ricerca per la colonna Prodotto
        col_prodotto = next((c for c in df_trans.columns if 'product' in c or 'prodotto' in c or 'articolo' in c or 'desc' in c), None)
        
        if col_target:
            stats_visite = df_trans[col_target].apply(extract_pv).dropna().value_counts().to_dict()
            if col_targa and col_qta:
                df_trans[col_qta] = df_trans[col_qta].apply(to_float)
                
                # --- CALCOLO VOLUMI GLOBALI (Per Action Plan) ---
                grouped = df_trans.groupby([col_target, col_targa])[col_qta].sum().reset_index()
                for _, row in grouped.iterrows():
                    pv = extract_pv(row[col_target])
                    if pv and row[col_qta] > 0:
                        client_volumes.append({
                            "pv": pv, "targa": str(row[col_targa]).strip(), "litri_storici": row[col_qta]
                        })
                
                # --- CALCOLO STORICO AUTISTI ---
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
                
                # Consolidamento prezzo medio
                for t in storico_autisti:
                    for m in storico_autisti[t]:
                        for s in storico_autisti[t][m]:
                            for pv in storico_autisti[t][m][s]:
                                for prod in storico_autisti[t][m][s][pv]:
                                    d = storico_autisti[t][m][s][pv][prod]
                                    d['prezzo_medio'] = d['spesa_tot'] / d['litri'] if d['litri'] > 0 else 0
                                    del d['spesa_tot']

    except Exception as e: print(f"⚠️ Errore lettura consumi: {e}")

    # 3. Caricamento Anagrafiche
    print("Caricamento anagrafiche WEX...")
    try:
        df_coords = pd.read_excel(FILE_EXCEL_ANAGRAFICA)
        df_wex = pd.read_csv(FILE_INCROCIO, sep=';')
        db = pd.merge(df_coords, df_wex[['PUNTO VENDITA', 'idImpianto']], on='PUNTO VENDITA', how='left')
        db['idImpianto'] = db['idImpianto'].astype(str).str.strip()
    except Exception as e: return

    # 4. Sync Ministero
    prezzi_live = {}
    prodotti_set = set()
    print("⏳ Sincronizzazione PREZZI ODIERNI Ministero (90s timeout)...")
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
    except: pass
    if not prodotti_set: prodotti_set = {"Gasolio Autotrazione (Self)", "Gasolio (Self)", "Benzina (Self)"}

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
            
            /* TABS CSS */
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
                <div style="font-size:9px; color:#94a3b8; margin-top:4px;">*I distributori con prezzi più vecchi verranno automaticamente eliminati.</div>
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
            <h3 style="font-size:12px; color:#94a3b8; margin-top:15px; text-transform:uppercase;">Stazioni Attive (Prezzi Recenti)</h3>
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
                    <div style="font-size:10px; color:var(--p);">Garantito spostando oggi i volumi abituali</div>
                </div>
                
                <h3 style="font-size:12px; color:#94a3b8; text-transform:uppercase; border-bottom:1px solid rgba(255,255,255,0.1); padding-bottom:5px;">Nuove Direttive per Targa</h3>
                <div style="font-size:10px; color:var(--p); margin-bottom:10px; font-style:italic;">💡 Clicca sul riquadro per mostrare al cliente la deviazione</div>
                <div id="targhe-list"></div>
            </div>
            
            <div id="tab-history" class="tab-content">
                <h3 style="font-size:12px; color:#94a3b8; text-transform:uppercase; border-bottom:1px solid rgba(255,255,255,0.1); padding-bottom:5px;">Abitudini di Rifornimento</h3>
                <div style="font-size:10px; color:var(--warn); margin-bottom:10px; font-style:italic;">📊 Analisi dei volumi per mese, settimana e tipologia di prodotto.</div>
                <div id="storico-list"></div>
            </div>
        </div>
        
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
        
        <script>
            const allStations = {json.dumps(marker_list)};
            const clientVolumes = {json.dumps(client_volumes)};
            const driverHistory = {json.dumps(storico_autisti)};
            
            const map = L.map('map', {{zoomControl: true}}).setView([42.0, 12.5], 6);
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png').addTo(map);
            let cluster = L.markerClusterGroup({{disableClusteringAtZoom: 12}}).addTo(map);
            let activeLines = L.layerGroup().addTo(map);
            
            let currentCenter = null;
            let showClientOnly = false;

            function switchTab(tabId) {{
                document.getElementById('tab-btn-plan').classList.remove('active');
                document.getElementById('tab-btn-plan').style.backgroundColor = 'transparent';
                document.getElementById('tab-btn-history').classList.remove('active');
                document.getElementById('tab-btn-history').style.backgroundColor = 'transparent';
                
                document.getElementById('tab-plan').classList.remove('active');
                document.getElementById('tab-history').classList.remove('active');
                
                const activeBtn = document.getElementById('tab-btn-' + tabId);
                activeBtn.classList.add('active');
                activeBtn.style.backgroundColor = 'var(--accent)';
                
                document.getElementById('tab-' + tabId).classList.add('active');
            }}

            window.highlightRoute = function(lat1, lng1, lat2, lng2) {{
                activeLines.clearLayers();
                const line = L.polyline([[lat1, lng1], [lat2, lng2]], {{ color: '#10b981', weight: 5, dashArray: '10, 10', opacity: 0.9 }}).addTo(activeLines);
                L.circleMarker([lat2, lng2], {{ radius: 12, fillColor: '#10b981', color:'#fff', weight:3, fillOpacity:1 }}).addTo(activeLines).bindPopup("<b style='color:#10b981; font-size:14px;'>🎯 NUOVA DIRETTIVA WEX</b>").openPopup();
                map.fitBounds(line.getBounds().pad(0.3));
            }};

            function toggleClientOnly() {{
                showClientOnly = !showClientOnly;
                const btn = document.getElementById('btn-client-only');
                if(showClientOnly) {{
                    btn.classList.add('active');
                    btn.innerText = '🔄 MOSTRA TUTTA LA RETE WEX';
                }} else {{
                    btn.classList.remove('active');
                    btn.innerText = '📍 FILTRA SOLO RETE CLIENTE';
                }}
                render();
            }}

            async function cercaSingolaCitta() {{
                const q = document.getElementById('loc').value;
                if(!q) return;
                activeLines.clearLayers();
                try {{
                    const r = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${{q}}&countrycodes=it&limit=1`).then(res => res.json());
                    if(r.length > 0) {{ 
                        currentCenter = [parseFloat(r[0].lat), parseFloat(r[0].lon)]; 
                        map.setView(currentCenter, 12); 
                        render();
                    }} else {{ alert("Località non trovata."); }}
                }} catch(err) {{ alert("Errore GPS."); }}
            }}

            function toggleTarga(id) {{ document.getElementById('targa-body-' + id).classList.toggle('open'); }}
            function toggleHistory(idx) {{ document.getElementById('hist-body-' + idx).classList.toggle('open'); }}

            function getValidStations(fType, maxAge) {{
                const now = Math.floor(Date.now() / 1000);
                return allStations.filter(s => {{
                    const f = s.fuels[fType];
                    if (!f || f.p <= 0 || f.ts === 0) return false;
                    if ((now - f.ts) > maxAge) return false; 
                    return true;
                }});
            }}

            // RENDER SCHEDA 2: STORICO AVANZATO
            function renderHistory() {{
                const histEl = document.getElementById('storico-list');
                histEl.innerHTML = "";
                
                if (Object.keys(driverHistory).length === 0) {{
                    histEl.innerHTML = "<div style='color:#94a3b8; font-size:11px; text-align:center; padding:20px;'>Nessun dato storico di prezzo o data trovato nel file.</div>";
                    return;
                }}
                
                const pvMap = {{}};
                allStations.forEach(s => pvMap[s.pv] = s.brand + " (" + s.addr + ")");

                let targaIdx = 0;
                for (const targa in driverHistory) {{
                    let monthsHtml = "";
                    const months = Object.keys(driverHistory[targa]).sort().reverse();
                    
                    months.forEach(m => {{
                        const labelMese = m === 'N.D.' ? 'Data Sconosciuta' : m;
                        
                        let litriMese = 0;
                        let weeksHtml = "";
                        const weeks = Object.keys(driverHistory[targa][m]).sort().reverse();
                        
                        weeks.forEach(w => {{
                            const labelSettimana = w === 'N.D.' ? 'Settimana Sconosciuta' : `Settimana ${{w}}`;
                            let litriSettimana = 0;
                            let pvsHtml = "";
                            
                            const pvs = Object.keys(driverHistory[targa][m][w]);
                            
                            pvs.forEach(pv => {{
                                const stationName = pvMap[pv] || `Stazione (Cod. ${{pv}})`;
                                const prodotti = Object.keys(driverHistory[targa][m][w][pv]);
                                
                                prodotti.forEach(prod => {{
                                    const data = driverHistory[targa][m][w][pv][prod];
                                    litriSettimana += data.litri;
                                    
                                    const pMedio = data.prezzo_medio > 0 ? `€ ${{data.prezzo_medio.toFixed(3)}}/L` : 'N.D.';
                                    const colorPrice = data.prezzo_medio > 0 ? '#fbbf24' : '#94a3b8';
                                    
                                    pvsHtml += `
                                        <div style="background:rgba(0,0,0,0.3); padding:8px; border-radius:6px; margin-top:5px; margin-left:15px; font-size:11px; border-left:3px solid #64748b;">
                                            <div style="color:#e2e8f0; font-weight:bold; margin-bottom:4px; display:flex; justify-content:space-between;">
                                                <span>📍 ${{stationName}}</span>
                                                <span style="color:var(--p); font-size:9px; border:1px solid var(--p); border-radius:3px; padding:1px 4px;">${{prod}}</span>
                                            </div>
                                            <div style="display:flex; justify-content:space-between; color:#94a3b8; align-items:center;">
                                                <span>Volumi: <b style="color:white; font-size:12px;">${{data.litri.toFixed(0)}} L</b></span>
                                                <span style="background:rgba(255,255,255,0.05); padding:3px 6px; border-radius:4px;">Prezzo pagato: <b style="color:${{colorPrice}}; font-size:12px;">${{pMedio}}</b></span>
                                            </div>
                                        </div>
                                    `;
                                }});
                            }});
                            
                            litriMese += litriSettimana;
                            
                            weeksHtml += `
                                <div style="margin-top:12px; margin-left: 10px; font-weight:bold; color:#a78bfa; font-size:11px; text-transform:uppercase; display:flex; justify-content:space-between; align-items:flex-end;">
                                    <span>🗓️ ${{labelSettimana}}</span>
                                    <span style="color:#e2e8f0; font-size:10px;">Totale: <span style="color:white; font-size:13px;">${{litriSettimana.toFixed(0)}} L</span></span>
                                </div>
                                ${{pvsHtml}}
                            `;
                        }});
                        
                        monthsHtml += `
                            <div style="margin-top:15px; font-weight:900; color:#38bdf8; font-size:14px; border-bottom:1px solid rgba(56, 189, 248, 0.3); padding-bottom:4px; padding-left:5px; display:flex; justify-content:space-between; align-items:flex-end;">
                                <span>📅 MESE: ${{labelMese}}</span>
                                <span style="color:#f8fafc; font-size:10px; font-weight:bold;">TOTALE: <span style="color:#38bdf8; font-size:15px;">${{litriMese.toFixed(0)}} L</span></span>
                            </div>
                            ${{weeksHtml}}
                        `;
                    }});

                    histEl.innerHTML += `
                        <div class="targa-card history">
                            <div class="targa-header" onclick="toggleHistory('${{targaIdx}}')">
                                <div>🚛 ${{targa}}</div>
                                <div style="font-size:10px; color:#94a3b8; font-weight:normal;">Dettagli Storico ▼</div>
                            </div>
                            <div class="targa-body" id="hist-body-${{targaIdx}}">
                                ${{monthsHtml}}
                            </div>
                        </div>
                    `;
                    targaIdx++;
                }}
            }}

            function render() {{
                const fType = document.getElementById('fuel').value;
                const rad = parseInt(document.getElementById('radius').value);
                const maxAge = parseInt(document.getElementById('freshness').value);
                
                cluster.clearLayers(); activeLines.clearLayers();
                const list = document.getElementById('res'); list.innerHTML = "";

                const validStations = getValidStations(fType, maxAge);

                let areaStations = validStations.filter(s => {{
                    if(currentCenter && rad < 9000000) return map.distance([s.lat, s.lng], currentCenter) <= rad;
                    return true;
                }});

                const validPrices = areaStations.map(s => s.fuels[fType].p);
                const minPrice = validPrices.length > 0 ? Math.min(...validPrices) : 0;

                areaStations.sort((a,b) => (b.visite - a.visite) || a.fuels[fType].p - b.fuels[fType].p).forEach(s => {{
                    const isClient = s.visite > 0;
                    if(showClientOnly && !isClient) return;

                    const f = s.fuels[fType];
                    const mColor = isClient ? '#ef4444' : '#3b82f6';
                    const m = L.circleMarker([s.lat, s.lng], {{ radius: isClient?10:6, fillColor: mColor, color:'#fff', weight:1, fillOpacity:0.9 }}).addTo(cluster);
                    
                    let bestAlt = null;
                    if(isClient && f.p > minPrice) {{
                        bestAlt = areaStations.find(alt => alt.fuels[fType].p === minPrice);
                    }}

                    let targheHtml = "";
                    if (isClient) {{
                        let targheInPV = clientVolumes.filter(cv => cv.pv === s.pv);
                        if (targheInPV.length > 0) {{
                            targheHtml = `<div class="targhe-box">
                                <div style="color:var(--client); font-weight:bold; margin-bottom:5px;">CAMION ABITUALI QUI:</div>`;
                            targheInPV.sort((a,b) => b.litri_storici - a.litri_storici).forEach(cv => {{
                                targheHtml += `<div class="targhe-row"><span>${{cv.targa}}</span> <b>${{cv.litri_storici.toFixed(0)}} L</b></div>`;
                            }});
                            targheHtml += `</div>`;
                        }}
                    }}

                    m.bindPopup(`<div style="text-align:center;"><b>${{s.brand}}</b><br><span style="font-size:18px; color:${{mColor}}; font-weight:bold;">${{f.p.toFixed(3)}}€</span><br><span style="font-size:9px; color:#94a3b8;">Aggiornato: ${{f.d}}</span></div>${{targheHtml}}`);
                    
                    const c = document.createElement('div');
                    c.className = `card ${{isClient ? 'active' : ''}}`;
                    c.innerHTML = `
                        ${{isClient ? `<div style="color:var(--client); font-size:10px; font-weight:900; margin-bottom:5px;">⚠️ STAZIONE ABITUALE</div>`:''}}
                        <div style="display:flex; justify-content:space-between; align-items:flex-end;">
                            <div><b style="font-size:13px;">${{s.brand}}</b><br><small style="color:#94a3b8;">${{s.addr}}</small><br><small style="color:#10b981; font-size:9px;">Aggiornato: ${{f.d}}</small></div>
                            <div class="price" style="font-size:18px;">${{f.p.toFixed(3)}}€</div>
                        </div>
                        ${{targheHtml}}
                        ${{bestAlt ? `<div style="color:var(--p); font-size:10px; margin-top:5px; font-weight:bold; padding-top:5px; border-top:1px solid rgba(255,255,255,0.1);">💡 Consigliato: ${{bestAlt.brand}} (-${{(f.p - bestAlt.fuels[fType].p).toFixed(3)}}€/L)</div>` : ''}}
                    `;
                    
                    c.onclick = () => {{ 
                        if(bestAlt) {{
                            activeLines.clearLayers();
                            L.polyline([[s.lat, s.lng], [bestAlt.lat, bestAlt.lng]], {{ color: '#10b981', weight: 4, dashArray: '10, 10', opacity: 0.8 }}).addTo(activeLines);
                            if(showClientOnly) {{
                                L.circleMarker([bestAlt.lat, bestAlt.lng], {{ radius: 8, fillColor: '#10b981', color:'#fff', weight:2, fillOpacity:1 }}).addTo(activeLines).bindPopup(`<b>${{bestAlt.brand}}</b><br>${{bestAlt.fuels[fType].p.toFixed(3)}}€`).openPopup();
                            }}
                        }}
                        map.setView([s.lat, s.lng], 15);
                        m.openPopup(); 
                    }};
                    list.appendChild(c);
                }});

                calculateDashboard(validStations);
            }}

            function calculateDashboard(validStations) {{
                if(clientVolumes.length === 0) return;
                const fType = document.getElementById('fuel').value;
                const rad = parseInt(document.getElementById('radius').value);
                
                let targaData = {{}};
                let totalSavings = 0;
                
                const stationByPv = {{}}; 
                validStations.forEach(s => stationByPv[s.pv] = s);

                clientVolumes.forEach(cv => {{
                    const cStat = stationByPv[cv.pv];
                    if(!cStat) return; 
                    const cPrice = cStat.fuels[fType].p;

                    let bestAlt = null;
                    let bestDist = 0;
                    
                    validStations.forEach(s => {{
                        if (s.pv === cStat.pv) return;
                        const dist = map.distance([cStat.lat, cStat.lng], [s.lat, s.lng]);
                        if (dist <= rad) {{
                            if (!bestAlt || s.fuels[fType].p < bestAlt.price) {{
                                bestAlt = {{ brand: s.brand, addr: s.addr, price: s.fuels[fType].p, lat: s.lat, lng: s.lng }};
                                bestDist = dist / 1000;
                            }}
                        }}
                    }});

                    if(bestAlt && bestAlt.price < cPrice) {{
                        if(!targaData[cv.targa]) targaData[cv.targa] = {{ savings: 0, plans: [] }};
                        
                        const risparmioLt = cPrice - bestAlt.price;
                        const risparmioVol = risparmioLt * cv.litri_storici;
                        
                        totalSavings += risparmioVol;
                        targaData[cv.targa].savings += risparmioVol;
                        
                        targaData[cv.targa].plans.push({{
                            vol: cv.litri_storici, 
                            from: cStat.brand, pFrom: cPrice, fLat: cStat.lat, fLng: cStat.lng,
                            to: bestAlt.brand, pTo: bestAlt.price, addr: bestAlt.addr, dist: bestDist, tLat: bestAlt.lat, tLng: bestAlt.lng,
                            gain: risparmioVol
                        }});
                    }}
                }});

                document.getElementById('tot-savings').innerText = `+ € ${{totalSavings.toFixed(2)}}`;
                
                const listEl = document.getElementById('targhe-list');
                listEl.innerHTML = "";
                
                const sortedTarghe = Object.keys(targaData).sort((a,b) => targaData[b].savings - targaData[a].savings);
                
                sortedTarghe.forEach((t, idx) => {{
                    const data = targaData[t];
                    if(data.savings <= 0) return;
                    
                    let plansHtml = "";
                    data.plans.forEach(p => {{
                        plansHtml += `
                            <div class="plan-item" onclick="highlightRoute(${{p.fLat}}, ${{p.fLng}}, ${{p.tLat}}, ${{p.tLng}})">
                                <div style="color:#ef4444; text-decoration:line-through; font-size:10px;">❌ Da: ${{p.from}} (${{p.pFrom.toFixed(3)}}€)</div>
                                <div style="color:var(--p); font-size:13px; font-weight:900; margin-top:6px;">✅ Vai a: ${{p.to}} (${{p.pTo.toFixed(3)}}€)</div>
                                <div style="color:#94a3b8; font-size:10px; margin-top:3px;">📍 ${{p.addr}} <span style="color:#cbd5e1;">(${{p.dist.toFixed(1)}} km)</span></div>
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-top:8px; border-top:1px dashed rgba(255,255,255,0.1); padding-top:6px;">
                                    <span style="font-size:10px; color:#cbd5e1;">Volumi stimati: ${{p.vol.toFixed(0)}} L</span>
                                    <span style="color:var(--p); font-weight:900; font-size:14px;">+ € ${{p.gain.toFixed(2)}}</span>
                                </div>
                            </div>
                        `;
                    }});

                    listEl.innerHTML += `
                        <div class="targa-card">
                            <div class="targa-header" onclick="toggleTarga('p-${{idx}}')">
                                <div>🚛 ${{t}}</div>
                                <div class="targa-gain">+ € ${{data.savings.toFixed(2)}}</div>
                            </div>
                            <div class="targa-body" id="targa-body-p-${{idx}}">
                                ${{plansHtml}}
                            </div>
                        </div>
                    `;
                }});
            }}

            function exportOptimizationPlan() {{
                if(clientVolumes.length === 0) return alert("Nessun dato volumetrico trovato.");
                const fType = document.getElementById('fuel').value;
                const rad = parseInt(document.getElementById('radius').value);
                const maxAge = parseInt(document.getElementById('freshness').value);
                
                const validStations = getValidStations(fType, maxAge);
                const stationByPv = {{}}; validStations.forEach(s => stationByPv[s.pv] = s);

                const report = [];
                clientVolumes.forEach(cv => {{
                    const cStat = stationByPv[cv.pv];
                    if(!cStat) return; 
                    const cPrice = cStat.fuels[fType].p;
                    
                    let bestAlt = null;
                    validStations.forEach(s => {{
                        if (s.pv === cStat.pv) return;
                        const dist = map.distance([cStat.lat, cStat.lng], [s.lat, s.lng]);
                        if (dist <= rad) {{
                            if (!bestAlt || s.fuels[fType].p < bestAlt.price) {{
                                bestAlt = {{ brand: s.brand, addr: s.addr, price: s.fuels[fType].p, dist: dist/1000 }};
                            }}
                        }}
                    }});

                    if(bestAlt && bestAlt.price < cPrice) {{
                        const rispLt = cPrice - bestAlt.price;
                        report.push({{
                            "Targa Veicolo": cv.targa,
                            "Stazione Abituale": cStat.brand,
                            "Volume Storico Base (L)": cv.litri_storici.toFixed(2),
                            "Prezzo Odierno Certificato (€/L)": cPrice,
                            "Alternativa Consigliata WEX": bestAlt.brand,
                            "Indirizzo Alternativa": bestAlt.addr,
                            "Distanza (KM)": bestAlt.dist.toFixed(1),
                            "Prezzo Odierno Alternativa (€/L)": bestAlt.price,
                            "Vantaggio al Litro (€/L)": rispLt.toFixed(3),
                            "Risparmio Netto Garantito (€)": (rispLt * cv.litri_storici).toFixed(2)
                        }});
                    }}
                }});
                
                if(report.length === 0) return alert("Con i filtri attuali non ci sono ottimizzazioni garantite da esportare.");
                report.sort((a,b) => b["Risparmio Netto Garantito (€)"] - a["Risparmio Netto Garantito (€)"]);
                
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(report), "Azione_su_Targhe_Certificata");
                XLSX.writeFile(wb, "Piano_Operativo_Flotta.xlsx");
            }}

            // Start
            renderHistory();
            render();
        </script>
    </body>
    </html>
    """
    
    path = os.path.abspath("WEX_REALTIME_PRECISION.html")
    with open(path, "w", encoding="utf-8") as f: f.write(html_template)
    
    chrome = 'C:/Program Files/Google/Chrome/Application/chrome.exe %s'
    try: webbrowser.get(chrome).open("file://" + path)
    except: webbrowser.open("file://" + path)
    print("✅ Piattaforma ottimizzazione pronta! (Inclusa Analisi per Mese, Settimana e Prodotto)")

if __name__ == "__main__":
    genera_wex_realtime_precision()