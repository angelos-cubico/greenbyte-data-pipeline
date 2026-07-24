r"""
One-file Cubico monthly appendix generator.

Reads everything from Azure Blob and uploads only the final appendix PDFs to SharePoint via Microsoft Graph.
Power-curve PNGs are temporary/local cache only and are NOT uploaded to SharePoint.

Required API_key.env values:
AZURE_STORAGE_CONNECTION_STRING=...
SP_TENANT_ID=...
SP_CLIENT_ID=...
SP_CLIENT_SECRET=...

Optional API_key.env values:
SIGNALS_CONTAINER_NAME=signals
STATUS_LOGS_CONTAINER_NAME=statuslogs
POWER_CURVES_CONTAINER_NAME=powercurves
POWER_CURVES_BLOB_PREFIX=power_curves
POWER_CURVE_TIMESTAMP=2026-01-01
POWER_CURVE_FILE_CONTAINS=manufacturer_power_curves
DATA_RESOLUTION=hourly
OUTPUT_FOLDER=appendix_output
SP_HOSTNAME=cubicoinvest.sharepoint.com
SP_SITE_PATH=/sites/CUBICOGREECETEAM2
SP_LIBRARY_NAME=Documents
SP_BASE_FOLDER=Technical Asset Management/5. CSI Greece/Reporting_AC

Run locally, upload to SharePoint via Graph:
python generate_appendices_common_graph.py --year 2026 --month 6 --asset all

Debug locally without SharePoint upload:
python generate_appendices_common_graph.py --year 2026 --month 6 --asset Avloi --upload-mode debug
"""
from __future__ import annotations

import argparse
import calendar
import os
import tempfile
from datetime import date
from io import BytesIO
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, Image

BASE_DIR = Path(__file__).parent
env_file = BASE_DIR / "API_key.env"

if env_file.exists():
    load_dotenv(env_file)

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING missing from API_key.env")

SIGNALS_CONTAINER_NAME = os.getenv("SIGNALS_CONTAINER_NAME", "signals")
STATUS_LOGS_CONTAINER_NAME = os.getenv("STATUS_LOGS_CONTAINER_NAME", "statuslogs")
POWER_CURVES_CONTAINER_NAME = os.getenv("POWER_CURVES_CONTAINER_NAME", "powercurves")
POWER_CURVES_BLOB_PREFIX = os.getenv("POWER_CURVES_BLOB_PREFIX", "power_curves").strip("/")
POWER_CURVE_TIMESTAMP = os.getenv("POWER_CURVE_TIMESTAMP", "2026-01-01")
POWER_CURVE_FILE_CONTAINS = os.getenv("POWER_CURVE_FILE_CONTAINS", "manufacturer_power_curves")
DATA_RESOLUTION = os.getenv("DATA_RESOLUTION", "hourly")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "appendix_output")
PLOT_CACHE_ROOT = Path(os.getenv("PLOT_CACHE_ROOT", str(BASE_DIR / OUTPUT_FOLDER / "_power_curve_plot_cache")))

SP_TENANT_ID = os.getenv("SP_TENANT_ID")
SP_CLIENT_ID = os.getenv("SP_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")
SP_HOSTNAME = os.getenv("SP_HOSTNAME", "cubicoinvest.sharepoint.com")
SP_SITE_PATH = os.getenv("SP_SITE_PATH", "/sites/CUBICOGREECETEAM2")
SP_LIBRARY_NAME = os.getenv("SP_LIBRARY_NAME", "Documents")
SP_BASE_FOLDER = os.getenv("SP_BASE_FOLDER", "Technical Asset Management/5. CSI Greece/Reporting_AC").strip("/")
GRAPH = "https://graph.microsoft.com/v1.0"

REPORTING_ASSETS = {
    "Avloi": ["avloi"],
    "Kitheronas 1": ["kitheronas_1"],
    "Kitheronas 2": ["kitheronas_2"],
    "Panachaiko 1": ["panachaiko_1"],
    "Panachaiko 2": ["panachaiko_2"],
    "Zarakes": ["koupia", "rachi_gioni", "tourla"],
}

DEVICE_LABELS = {
    24844:"AV01",24845:"AV02",24846:"AV03",24847:"AV04",
    24617:"STE01",24618:"STE02",24619:"STE03",24620:"STE04",24621:"STE05",24622:"STE06",24623:"STE07",
    24710:"STEII01",24711:"STEII02",24712:"STEII03",24713:"STEII04",
    24662:"PANI01",24663:"PANI02",24664:"PANI03",24665:"PANI04",24666:"PANI05",24667:"PANI06",24668:"PANI07",24669:"PANI08",24670:"PANI09",24671:"PANI10",24672:"PANI11",24673:"PANI12",24674:"PANI13",24675:"PANI14",24676:"PANI15",24677:"PANI16",24678:"PANI17",24679:"PANI18",24680:"PANI19",24681:"PANI20",24682:"PANI21",24683:"PANI22",24684:"PANI23",24685:"PANI24",24686:"PANI25",24687:"PANI26",24688:"PANI27",24689:"PANI28",24690:"PANI29",24691:"PANI30",24692:"PANI31",24693:"PANI32",24694:"PANI33",24695:"PANI34",24696:"PANI35",24697:"PANI36",24698:"PANI37",24699:"PANI38",24700:"PANI39",24701:"PANI40",24702:"PANI41",
    24644:"PANII01",24645:"PANII02",24646:"PANII03",24647:"PANII04",24648:"PANII05",24649:"PANII06",24650:"PANII07",24651:"PANII08",24652:"PANII09",24653:"PANII10",24654:"PANII11",24655:"PANII12",24656:"PANII13",24657:"PANII14",24658:"PANII15",24659:"PANII16",
    24715:"KP01",24716:"KP02",24717:"KP03",24718:"KP04",24719:"KP05",24720:"KP06",
    24721:"RG01",24722:"RG02",24723:"RG03",24724:"RG04",24725:"RG05",24726:"RG06",24727:"RG07",24728:"RG08",24729:"RG09",24730:"RG10",24731:"RG11",24732:"RG12",
    24733:"TR01",24734:"TR02",24735:"TR03",24736:"TR04",24737:"TR05",24738:"TR06",24739:"TR07",24740:"TR08",24741:"TR09",24742:"TR10",24743:"TR11",
}

TURBINES_PER_ENERGY_BLOCK = 5
STOP_ROWS_PER_PAGE = 26
POWER_CURVE_PLOTS_PER_PAGE = 4
MAX_SCATTER_POINTS = 5000
BIN_WIDTH = 0.5
MIN_POINTS_PER_BIN = 3
MAX_MESSAGE_CHARS = 95

# ---------- helpers ----------
def month_key(y:int,m:int)->str: return f"{y}-{m:02d}"
def month_label(y:int,m:int)->str: return f"{calendar.month_name[m]} {y}"
def month_abbr(m:int)->str: return calendar.month_abbr[m]
def safe_name(x:str)->str: return str(x).strip().replace(" ", "_")
def turbine_label(device_id)->str:
    try: return DEVICE_LABELS.get(int(device_id), str(device_id))
    except Exception: return str(device_id)
def previous_calendar_month(today:date|None=None)->tuple[int,int]:
    today = today or date.today()
    return (today.year-1,12) if today.month==1 else (today.year,today.month-1)
def clean_text(v,max_chars:int=MAX_MESSAGE_CHARS)->str:
    if pd.isna(v): return ""
    t=" ".join(str(v).replace("\n"," ").replace("\r"," ").strip().split())
    return t[:max_chars-3]+"..." if len(t)>max_chars else t
def fmt_number(v):
    if v=="": return ""
    if pd.isna(v): return "N/A"
    try: return f"{float(v):,.0f}".replace(","," ")
    except Exception: return str(v)
def fmt_decimal(v):
    if pd.isna(v): return ""
    try: return f"{float(v):,.2f}"
    except Exception: return ""
def fmt_datetime(v):
    if pd.isna(v): return ""
    try: return pd.to_datetime(v).strftime("%Y-%m-%d %H:%M")
    except Exception: return str(v)

def select_reporting_assets(asset:str)->dict[str,list[str]]:
    if asset.lower()=="all": return REPORTING_ASSETS
    if asset in REPORTING_ASSETS: return {asset: REPORTING_ASSETS[asset]}
    norm=asset.lower().replace(" ","_")
    for k,v in REPORTING_ASSETS.items():
        if norm in v: return {k:v}
    raise ValueError("Unknown asset. Valid: "+", ".join(REPORTING_ASSETS)+", all")

# ---------- Azure Blob ----------
def blob_service()->BlobServiceClient: return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
def list_blobs(container:str,prefix:str)->list[str]: return [b.name for b in blob_service().get_container_client(container).list_blobs(name_starts_with=prefix)]
def read_parquet_blob(container:str, blob:str)->pd.DataFrame:
    buf=BytesIO(); blob_service().get_blob_client(container=container, blob=blob).download_blob().readinto(buf); buf.seek(0); return pd.read_parquet(buf)
def signal_blob_name(asset:str,y:int,m:int)->str:
    fn=f"{asset}_signals_{y}_{m:02d}_{DATA_RESOLUTION}_powerbi.parquet"; return f"asset={asset}/year={y}/month={m:02d}/{fn}"
def find_first_parquet_blob(container:str,prefixes:list[str],contains:str|None=None,timestamp:str|None=None)->str|None:
    for prefix in prefixes:
        c=[]
        for name in list_blobs(container,prefix):
            low=name.lower()
            if not low.endswith(".parquet"): continue
            if contains and contains.lower() not in low: continue
            if timestamp and timestamp not in name: continue
            c.append(name)
        if c: return sorted(c)[0]
    return None

def load_signals_month(asset_folders:list[str],y:int,m:int)->pd.DataFrame:
    frames=[]
    for a in asset_folders:
        b=signal_blob_name(a,y,m); print("Reading signal blob:",b)
        try: df=read_parquet_blob(SIGNALS_CONTAINER_NAME,b)
        except Exception as e: print(f"WARNING signal blob missing {a}: {e}"); continue
        df["SourceAssetFolder"]=a; frames.append(df)
    return standardize_power_signal_dataframe(pd.concat(frames,ignore_index=True)) if frames else pd.DataFrame()

def load_signals_ytd(asset_folders:list[str],y:int,m:int)->pd.DataFrame:
    frames=[]
    for a in asset_folders:
        for mm in range(1,m+1):
            try: df=read_parquet_blob(SIGNALS_CONTAINER_NAME,signal_blob_name(a,y,mm))
            except Exception: continue
            df["SourceAssetFolder"]=a; frames.append(df)
    return standardize_energy_dataframe(pd.concat(frames,ignore_index=True)) if frames else pd.DataFrame()

def load_status_month(asset_folders:list[str],y:int,m:int)->pd.DataFrame:
    frames=[]
    for a in asset_folders:
        prefixes=[f"asset={a}/year={y}/month={m:02d}",f"status_logs/asset={a}/year={y}/month={m:02d}",f"greenbyte_backfill/status_logs/asset={a}/year={y}/month={m:02d}"]
        b=find_first_parquet_blob(STATUS_LOGS_CONTAINER_NAME,prefixes)
        if not b: print(f"WARNING no status log blob for {a}"); continue
        print("Reading status log blob:",b); df=read_parquet_blob(STATUS_LOGS_CONTAINER_NAME,b); df["SourceAssetFolder"]=a; frames.append(df)
    return pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()

def load_power_curves(asset_folders:list[str],timestamp:str)->pd.DataFrame:
    frames=[]
    for a in asset_folders:
        prefixes=[]
        if POWER_CURVES_BLOB_PREFIX: prefixes.append(f"{POWER_CURVES_BLOB_PREFIX}/asset={a}")
        prefixes += [f"asset={a}",f"power_curves/asset={a}",f"greenbyte_backfill/power_curves/asset={a}"]
        b=find_first_parquet_blob(POWER_CURVES_CONTAINER_NAME,prefixes,POWER_CURVE_FILE_CONTAINS,timestamp) or find_first_parquet_blob(POWER_CURVES_CONTAINER_NAME,prefixes,POWER_CURVE_FILE_CONTAINS)
        if not b: print(f"WARNING no power curve blob for {a}"); continue
        print("Reading power curve blob:",b); df=read_parquet_blob(POWER_CURVES_CONTAINER_NAME,b); frames.append(df)
    if not frames: return pd.DataFrame()
    out=pd.concat(frames,ignore_index=True)
    for col in ["DeviceID","WindSpeed","Power"]:
        if col in out.columns: out[col]=pd.to_numeric(out[col],errors="coerce")
    return out

# ---------- Graph ----------
def graph_token()->str:
    if not (SP_TENANT_ID and SP_CLIENT_ID and SP_CLIENT_SECRET): raise ValueError("SP_TENANT_ID/SP_CLIENT_ID/SP_CLIENT_SECRET missing")
    r=requests.post(f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token",data={"client_id":SP_CLIENT_ID,"client_secret":SP_CLIENT_SECRET,"scope":"https://graph.microsoft.com/.default","grant_type":"client_credentials"},timeout=60)
    print("Graph token status:",r.status_code); 
    if r.status_code>=400: print(r.text)
    r.raise_for_status(); return r.json()["access_token"]
def gget(url,token):
    r=requests.get(url,headers={"Authorization":f"Bearer {token}"},timeout=60); print("GET",url,"Status:",r.status_code)
    if r.status_code>=400: print(r.text)
    r.raise_for_status(); return r.json()
def gpost(url,token,body):
    r=requests.post(url,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},json=body,timeout=60); print("POST",url,"Status:",r.status_code)
    if r.status_code>=400: print(r.text)
    r.raise_for_status(); return r.json()
def gput(url,token,content:bytes,ctype="application/pdf"):
    r=requests.put(url,headers={"Authorization":f"Bearer {token}","Content-Type":ctype},data=content,timeout=180); print("PUT",url,"Status:",r.status_code)
    if r.status_code>=400: print(r.text)
    r.raise_for_status(); return r.json()
def get_site_id(token): return gget(f"{GRAPH}/sites/{SP_HOSTNAME}:{SP_SITE_PATH}",token)["id"]
def get_drive_id(site_id,token):
    drives=gget(f"{GRAPH}/sites/{site_id}/drives",token).get("value",[]); print("Available drives:",[(d.get('name'),d.get('id')) for d in drives])
    for d in drives:
        if d.get("name","").lower()==SP_LIBRARY_NAME.lower(): return d["id"]
    if len(drives)==1: return drives[0]["id"]
    raise ValueError(f"Could not find drive/library {SP_LIBRARY_NAME}")
def get_item_by_path(drive_id,path,token):
    clean=path.strip("/")
    if not clean: return gget(f"{GRAPH}/drives/{drive_id}/root",token)
    url=f"{GRAPH}/drives/{drive_id}/root:/{clean}"; r=requests.get(url,headers={"Authorization":f"Bearer {token}"},timeout=60)
    if r.status_code==404: return None
    print("GET",url,"Status:",r.status_code)
    if r.status_code>=400: print(r.text)
    r.raise_for_status(); return r.json()
def ensure_folder(drive_id,folder_path,token):
    parts=[p for p in folder_path.strip("/").split("/") if p]; cur=""; item=gget(f"{GRAPH}/drives/{drive_id}/root",token)
    for p in parts:
        nxt=f"{cur}/{p}".strip("/"); existing=get_item_by_path(drive_id,nxt,token)
        if existing is None:
            print("Creating SharePoint folder:",nxt)
            existing=gpost(f"{GRAPH}/drives/{drive_id}/items/{item['id']}/children",token,{"name":p,"folder":{},"@microsoft.graph.conflictBehavior":"replace"})
        item=existing; cur=nxt
def upload_pdf(drive_id,target_folder,local_file:Path,token):
    ensure_folder(drive_id,target_folder,token)
    path=f"{target_folder.strip('/')}/{local_file.name}"; url=f"{GRAPH}/drives/{drive_id}/root:/{path}:/content"
    result=gput(url,token,local_file.read_bytes()); print("Uploaded:",result.get("webUrl")); return result.get("webUrl","")

# ---------- transforms ----------
def standardize_power_signal_dataframe(df):
    if df.empty: return df
    df=df.copy();
    if "Timestamp" in df.columns: df["Timestamp"]=pd.to_datetime(df["Timestamp"],errors="coerce")
    if "Wind_Speed" in df.columns and "Power" in df.columns:
        df["Wind_Speed"]=pd.to_numeric(df["Wind_Speed"],errors="coerce"); df["Power"]=pd.to_numeric(df["Power"],errors="coerce"); return df
    if "Signal" in df.columns and "Value" in df.columns:
        t=df.copy(); t["SignalClean"]=t["Signal"].astype(str).str.strip().str.lower(); keep=t[t["SignalClean"].isin(["wind speed","power"])].copy()
        keep["SignalColumn"]=keep["SignalClean"].map({"wind speed":"Wind_Speed","power":"Power"}); idx=[c for c in ["Asset","WindFarm","SubPark","SourceAssetFolder","DeviceID","Timestamp"] if c in keep.columns]
        w=keep.pivot_table(index=idx,columns="SignalColumn",values="Value",aggfunc="first").reset_index(); w.columns.name=None
        w["Wind_Speed"]=pd.to_numeric(w.get("Wind_Speed"),errors="coerce"); w["Power"]=pd.to_numeric(w.get("Power"),errors="coerce"); return w
    raise ValueError("Signals need Wind_Speed+Power or long Signal+Value format")
def standardize_energy_dataframe(df):
    if df.empty: return df
    df=df.copy();
    if "Timestamp" in df.columns: df["Timestamp"]=pd.to_datetime(df["Timestamp"],errors="coerce")
    if "Month" not in df.columns: df["Month"]=df["Timestamp"].dt.month
    if "Energy_Export" in df.columns:
        df["EnergyExportValue"]=pd.to_numeric(df["Energy_Export"],errors="coerce"); return df
    if "Signal" in df.columns and "Value" in df.columns:
        t=df[df["Signal"].astype(str).str.strip().str.lower().eq("energy export")].copy(); t["EnergyExportValue"]=pd.to_numeric(t["Value"],errors="coerce"); return t
    raise ValueError("Signals need Energy_Export or long Signal+Value")
def normalize_status(df):
    if df.empty: return df
    df=df.copy(); ren={"timestampStart":"TimestampStart","timestampEnd":"TimestampEnd","deviceId":"DeviceID","category":"Category","code":"Code","message":"Message","lostProduction":"LostProduction","categoryGlobalContract":"CategoryGlobalContract"}; df=df.rename(columns={k:v for k,v in ren.items() if k in df.columns and v not in df.columns})
    for c in ["TimestampStart","TimestampEnd"]:
        if c in df.columns: df[c]=pd.to_datetime(df[c],errors="coerce")
    df["DurationHours"]=((df["TimestampEnd"]-df["TimestampStart"]).dt.total_seconds()/3600) if {"TimestampStart","TimestampEnd"}.issubset(df.columns) else pd.NA
    df["LostProduction_MWh"]=pd.to_numeric(df["LostProduction"],errors="coerce")/1000 if "LostProduction" in df.columns else pd.NA
    df["Turbine"]=df["DeviceID"].apply(turbine_label) if "DeviceID" in df.columns else ""
    return df
def filter_stops(df):
    if df.empty: return df
    cat=df.get("Category",pd.Series([""]*len(df))).astype(str).str.lower(); glob=df.get("CategoryGlobalContract",pd.Series([""]*len(df))).astype(str).str.lower(); out=df[cat.str.contains("stop",na=False)|glob.str.contains("stop",na=False)].copy()
    if "TimestampStart" in out.columns: out=out.sort_values("TimestampStart")
    return out.reset_index(drop=True)

def prepare_energy(signals,reporting_month):
    if signals.empty: return pd.DataFrame()
    df=signals.copy(); df["Turbine"]=df["DeviceID"].apply(turbine_label); monthly=df.groupby(["Month","Turbine"],dropna=False)["EnergyExportValue"].sum().reset_index(); pivot=monthly.pivot(index="Month",columns="Turbine",values="EnergyExportValue").reindex(range(1,13))
    for mm in range(reporting_month+1,13): pivot.loc[mm,:]=pd.NA
    pivot["Total"]=pivot.sum(axis=1,skipna=True)
    for mm in range(reporting_month+1,13): pivot.loc[mm,"Total"]=pd.NA
    pivot.insert(0,"MonthName",[month_abbr(mm) for mm in pivot.index]); nums=[c for c in pivot.columns if c!="MonthName"]; actual=pivot.loc[range(1,reporting_month+1),nums]
    stats=pd.DataFrame([actual.sum(),actual.min(),actual.mean(),actual.max()],index=["Total","Minimum","Average","Maximum"]); stats.insert(0,"MonthName",stats.index); blank=pd.DataFrame([{c:"" for c in pivot.columns}]); return pd.concat([pivot,blank,stats],ignore_index=True)

# ---------- PDF helpers ----------
def build_styles():
    sample=getSampleStyleSheet(); return {"title":ParagraphStyle("TitleCustom",parent=sample["Title"],fontName="Helvetica",fontSize=21,leading=25,alignment=TA_LEFT,textColor=colors.HexColor("#111111"),spaceAfter=10),"subtitle":ParagraphStyle("SubtitleCustom",parent=sample["Normal"],fontName="Helvetica",fontSize=8,leading=10,alignment=TA_LEFT,textColor=colors.HexColor("#555555"),spaceAfter=8),"cell":ParagraphStyle("CellCustom",parent=sample["Normal"],fontName="Helvetica",fontSize=6.7,leading=8,alignment=TA_RIGHT),"cell_left":ParagraphStyle("CellLeftCustom",parent=sample["Normal"],fontName="Helvetica",fontSize=6.7,leading=8,alignment=TA_LEFT),"header":ParagraphStyle("HeaderCustom",parent=sample["Normal"],fontName="Helvetica-Bold",fontSize=6.7,leading=8,alignment=TA_CENTER),"small":ParagraphStyle("SmallCustom",parent=sample["Normal"],fontName="Helvetica",fontSize=6.5,leading=8,alignment=TA_LEFT)}
def para(text,style): return Paragraph(str(text),style)
def footer(canvas,doc): canvas.saveState(); canvas.setFont("Helvetica",7); canvas.setFillColor(colors.HexColor("#666666")); canvas.drawString(1.1*cm,0.7*cm,"Monthly Reporting Appendix"); canvas.drawRightString(landscape(A4)[0]-1.1*cm,0.7*cm,f"Page {doc.page}"); canvas.restoreState()
def energy_table(df,cols,year,styles):
    visible=["MonthName"]+cols+["Total"]; data=[[""]+[para("Energy Export (kWh)",styles["header"])]*(len(visible)-1),[para(str(year),styles["header"])] + [para(c,styles["header"]) for c in cols]+[para("Total",styles["header"])] ]
    for _,r in df.iterrows(): data.append([para(r.get("MonthName",""),styles["cell_left"])] + [para(fmt_number(r.get(c,pd.NA)),styles["cell"]) for c in cols+["Total"]])
    page_width=landscape(A4)[0]-2.2*cm; first=2.5*cm; other=(page_width-first)/(len(visible)-1); t=Table(data,colWidths=[first]+[other]*(len(visible)-1),repeatRows=2); t.setStyle(TableStyle([("SPAN",(1,0),(-1,0)),("BACKGROUND",(0,0),(-1,1),colors.HexColor("#F2F2F2")),("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#B8B8B8")),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("BACKGROUND",(0,-4),(-1,-1),colors.HexColor("#FAFAFA")),("LINEABOVE",(0,-4),(-1,-4),0.6,colors.HexColor("#777777"))])); return t
def add_energy(story,asset,year,month,edf,styles):
    story += [para("Annex 1 - Turbine energy distribution",styles["title"]), para(f"Asset: {asset} | Year: {year} | Values shown in kWh",styles["subtitle"])]
    if edf.empty: story += [para("No Energy Export data found.",styles["small"]),PageBreak()]; return
    tcols=sorted([c for c in edf.columns if c not in ["MonthName","Total"]])
    for i in range(0,len(tcols),TURBINES_PER_ENERGY_BLOCK): story += [energy_table(edf,tcols[i:i+TURBINES_PER_ENERGY_BLOCK],year,styles), Spacer(1,0.3*cm)]
    story.append(PageBreak())
def stops_table(df,cols,widths,styles):
    data=[[para(c,styles["header"]) for c in cols]]
    for _,r in df.iterrows():
        row=[]
        for c in cols:
            v=r.get(c,""); v=fmt_datetime(v) if c in ["TimestampStart","TimestampEnd"] else fmt_decimal(v) if c in ["LostProduction_MWh","DurationHours"] else clean_text(v); row.append(para(v,styles["small"]))
        data.append(row)
    t=Table(data,colWidths=widths,repeatRows=1); t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F2F2F2")),("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#B8B8B8")),("VALIGN",(0,0),(-1,-1),"TOP"),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#FAFAFA")])])); return t
def add_stops(story,asset,year,month,stops,styles):
    story += [para("Annex 2 - Monthly stop logs",styles["title"]),para(f"Asset: {asset} | Reporting month: {month_label(year,month)} | Only stop events are included",styles["subtitle"])]
    if stops.empty: story += [para("No stop events found for this reporting month.",styles["small"]),PageBreak()]; return
    cols=[c for c in ["TimestampStart","TimestampEnd","Turbine","Code","Message","Category","LostProduction_MWh","DurationHours"] if c in stops.columns]; widths=[3*cm,3*cm,2*cm,1.9*cm,8.7*cm,2.2*cm,2.5*cm,2.3*cm][:len(cols)]
    for i in range(0,len(stops),STOP_ROWS_PER_PAGE):
        story.append(stops_table(stops.iloc[i:i+STOP_ROWS_PER_PAGE],cols,widths,styles))
        if i+STOP_ROWS_PER_PAGE < len(stops): story += [PageBreak(),para("Annex 2 - Monthly stop logs continued",styles["title"])]
    story.append(PageBreak())

def monthly_points(signals,device_id):
    df=signals[signals["DeviceID"].astype(str)==str(device_id)].copy();
    if df.empty: return df
    df["Wind_Speed"]=pd.to_numeric(df["Wind_Speed"],errors="coerce"); df["Power"]=pd.to_numeric(df["Power"],errors="coerce"); df=df.dropna(subset=["Wind_Speed","Power"]); df=df[(df["Wind_Speed"]>=0)&(df["Power"]>=0)]
    return df.sample(MAX_SCATTER_POINTS,random_state=42).sort_values("Wind_Speed") if len(df)>MAX_SCATTER_POINTS else df
def binned(points):
    if points.empty: return pd.DataFrame()
    d=points.copy(); d["WindSpeedBin"]=(d["Wind_Speed"]/BIN_WIDTH).round()*BIN_WIDTH; b=d.groupby("WindSpeedBin").agg(PowerMean=("Power","mean"),Count=("Power","count")).reset_index(); return b[b["Count"]>=MIN_POINTS_PER_BIN].sort_values("WindSpeedBin")
def man_curve(curves,device_id):
    if curves.empty: return curves
    d=curves[curves["DeviceID"].astype(str)==str(device_id)].copy(); return d.dropna(subset=["WindSpeed","Power"]).sort_values("WindSpeed") if not d.empty else d
def plot_curve(device_id,man,pts,png):
    fig,ax=plt.subplots(figsize=(6.8,2.2),dpi=170)
    if not pts.empty:
        ax.scatter(pts["Wind_Speed"],pts["Power"],s=4,color="#4EA3F7",alpha=0.48,linewidths=0,label="Monthly points"); bb=binned(pts)
        if not bb.empty: ax.plot(bb["WindSpeedBin"],bb["PowerMean"],color="#1976D2",linewidth=1.4,label="Monthly binned mean")
    if not man.empty: ax.plot(man["WindSpeed"],man["Power"],color="#555555",linewidth=1.3,label="Manufacturer curve")
    ax.set_title(turbine_label(device_id),fontsize=8,loc="left",pad=2); ax.set_xlabel("m/s",fontsize=6,loc="right"); ax.set_ylabel("kW",fontsize=6,rotation=0,labelpad=8,loc="top"); ax.grid(True,color="#D9D9D9",linewidth=0.5); ax.tick_params(axis="both",labelsize=5); ax.set_xlim(left=0); ax.set_ylim(bottom=0)
    mx=0
    if not man.empty: mx=max(mx,man["Power"].max())
    if not pts.empty: mx=max(mx,pts["Power"].quantile(0.995))
    if pd.notna(mx) and mx>0: ax.set_ylim(0,mx*1.10)
    ax.legend(loc="upper left",fontsize=5,frameon=False,handlelength=1.5); fig.tight_layout(pad=0.45); png.parent.mkdir(parents=True,exist_ok=True); fig.savefig(png,bbox_inches="tight"); plt.close(fig)
def add_curves(story,asset,year,month,signals,curves,styles,out_pdf):
    story += [para("Annex 3 - Power curves",styles["title"]), para(f"Asset: {asset} | Reporting month: {month_label(year,month)} | Blue points are monthly measured Wind Speed vs Power; grey line is manufacturer curve",styles["subtitle"])]
    if signals.empty: story.append(para("No signal data available for power curve plots.",styles["small"])); return
    plot_dir=PLOT_CACHE_ROOT/month_key(year,month)/safe_name(asset); plot_dir.mkdir(parents=True,exist_ok=True); paths=[]
    ids=sorted(signals["DeviceID"].dropna().unique()); print("Signal DeviceIDs in PDF builder:",[int(x) for x in ids])
    if not curves.empty and "DeviceID" in curves.columns: print("Curve DeviceIDs in PDF builder:",[int(x) for x in sorted(curves["DeviceID"].dropna().unique())])
    for did in ids:
        pts=monthly_points(signals,did); man=man_curve(curves,did); print(f"Turbine {did} / {turbine_label(did)} | signal points={len(pts)} | manufacturer points={len(man)}")
        if pts.empty and man.empty: continue
        png=plot_dir/f"pc_{did}.png"; plot_curve(did,man,pts,png); paths.append(png)
    if not paths: story.append(para("No turbine-level power curve plots could be created.",styles["small"])); return
    for i,p in enumerate(paths):
        story += [Image(str(p),width=16.5*cm,height=5.2*cm), Spacer(1,0.12*cm)]
        if (i+1)%POWER_CURVE_PLOTS_PER_PAGE==0 and i+1<len(paths): story += [PageBreak(),para("Annex 3 - Power curves continued",styles["title"])]

def generate_pdf(asset,year,month,edf,stops,monthly,curves,out_pdf):
    out_pdf.parent.mkdir(parents=True,exist_ok=True); styles=build_styles(); doc=SimpleDocTemplate(str(out_pdf),pagesize=landscape(A4),leftMargin=1.1*cm,rightMargin=1.1*cm,topMargin=1.1*cm,bottomMargin=1.2*cm); story=[]
    add_energy(story,asset,year,month,edf,styles); add_stops(story,asset,year,month,stops,styles); add_curves(story,asset,year,month,monthly,curves,styles,out_pdf); doc.build(story,onFirstPage=footer,onLaterPages=footer); print("Created appendix PDF:",out_pdf)

# ---------- main ----------
def parse_args():
    dy,dm=previous_calendar_month(); p=argparse.ArgumentParser(description="Generate appendix PDFs and upload to SharePoint via Graph")
    p.add_argument("--year",type=int,default=dy); p.add_argument("--month",type=int,default=dm); p.add_argument("--asset",type=str,default="all"); p.add_argument("--timestamp",type=str,default=POWER_CURVE_TIMESTAMP); p.add_argument("--upload-mode",choices=["graph","debug"],default="graph"); return p.parse_args()
def main():
    args=parse_args(); selected=select_reporting_assets(args.asset); report_month=month_key(args.year,args.month)
    if args.upload_mode=="graph":
        tok=graph_token(); site=get_site_id(tok); drive=get_drive_id(site,tok); target=f"{SP_BASE_FOLDER}/{report_month}/Appendices"; print("Graph upload target folder:",target); ensure_folder(drive,target,tok); out_dir=Path(tempfile.gettempdir())/"cubico_appendices"/report_month; out_dir.mkdir(parents=True,exist_ok=True)
    else:
        tok=drive=target=None; out_dir=BASE_DIR/OUTPUT_FOLDER/report_month/"Appendices"; out_dir.mkdir(parents=True,exist_ok=True)
    print("Generating appendices. Upload mode:",args.upload_mode); print("Working output dir:",out_dir); urls=[]
    for asset,folders in selected.items():
        print("="*80); print("Reporting asset:",asset); print("Underlying folders:",", ".join(folders))
        ytd=load_signals_ytd(folders,args.year,args.month); edf=prepare_energy(ytd,args.month); monthly=load_signals_month(folders,args.year,args.month)
        if not monthly.empty: print("Monthly signal DeviceIDs:",[int(x) for x in sorted(monthly["DeviceID"].dropna().unique())])
        status=load_status_month(folders,args.year,args.month); stops=filter_stops(normalize_status(status)); curves=load_power_curves(folders,args.timestamp)
        if not curves.empty: print("Power curve DeviceIDs:",[int(x) for x in sorted(curves["DeviceID"].dropna().unique())])
        pdf=out_dir/f"{report_month}_{safe_name(asset)}_Appendix.pdf"; generate_pdf(asset,args.year,args.month,edf,stops,monthly,curves,pdf)
        if args.upload_mode=="graph":
            urls.append(upload_pdf(drive,target,pdf,tok))
            try: pdf.unlink(); print("Deleted temp PDF:",pdf)
            except Exception as e: print("WARNING could not delete temp PDF:",e)
    print("="*80); print("DONE")
    if urls:
        print("Uploaded SharePoint URLs:"); [print(u) for u in urls]
if __name__=="__main__": main()
