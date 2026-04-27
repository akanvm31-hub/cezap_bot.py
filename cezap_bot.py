import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
from datetime import datetime

# --- CONFIGURATION ---
DATATOURISME_FLUX_URL = "https://diffuseur.datatourisme.fr/webservice/f5ba593fdfeb0297cc2f33aed8fb203f/f23e25c9-de75-481c-be0c-76beac417ade" 
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
DB_NAME = "cezap.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- BASE DE DONNÉES ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sent_alerts
                 (item_id TEXT PRIMARY KEY, client TEXT, date_sent TEXT)''')
    conn.commit()
    conn.close()

def is_new(item_id, client_nom):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT 1 FROM sent_alerts WHERE item_id = ? AND client = ?", (item_id, client_nom))
    res = c.fetchone()
    conn.close()
    return res is None

def save_alert(item_id, client_nom):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO sent_alerts VALUES (?, ?, ?)", 
              (item_id, client_nom, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# --- SOURCE 1 : RESTAURATION (Alim'confiance) ---
def get_alim_confiance(ville="Paris"):
    deals = []
    url = "https://dgal.opendatasoft.com/api/records/1.0/search/"
    params = {"dataset": "export_alimconfiance", "q": ville, "rows": 5, "refine.synthese_eval_sanitaire": "Très satisfaisant"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            for r in resp.json().get("records", []):
                f = r.get("fields", {})
                nom = f.get("app_libelle_etablissement")
                deal_id = "alim_" + hashlib.md5(f"{nom}".encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id, "titre": nom, "lieu": f.get("libelle_commune", ""),
                    "categorie": "Restaurant (Hygiène Top ★★★)", "image": None,
                    "url": f"https://www.google.com/search?q={nom}", "source": "Alim'confiance"
                })
    except Exception as e: logger.error(f"Erreur Alim: {e}")
    return deals

# --- SOURCE 2 : LOISIRS (DATAtourisme) ---
def get_datatourisme():
    deals = []
    try:
        resp = requests.get(DATATOURISME_FLUX_URL, timeout=15)
        if resp.status_code == 200:
            items = resp.json().get("@graph", [])
            for item in items[:10]:
                nom = item.get("rdfs:label", {}).get("@value")
                if not nom: continue
                deal_id = "data_" + hashlib.md5(nom.encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id, "titre": nom, 
                    "lieu": item.get("isLocatedAt", [{}]).get("schema:address", [{}]).get("schema:addressLocality", "IDF"),
                    "categorie": "Sortie & Loisirs", "image": None,
                    "url": item.get("@id", "https://www.datatourisme.fr"), "source": "DATAtourisme"
                })
    except Exception as e: logger.error(f"Erreur DATAtourisme: {e}")
    return deals

# --- SOURCE 3 : EVENEMENTS (OpenAgenda) ---
def get_open_agenda():
    deals = []
    # Ici on utilise l'agenda "Sortir en Île-de-France" (UID: 89803504)
    url = "https://openagenda.com/agendas/89803504/events.json"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            events = resp.json().get("events", [])
            for e in events[:5]:
                nom = e.get("title", {}).get("fr")
                deal_id = "open_" + str(e.get("uid"))
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": e.get("locationName", "IDF"),
                    "categorie": "Événement / Expo",
                    "image": e.get("image"),
                    "url": f"https://openagenda.com/event/{e.get('slug')}",
                    "source": "OpenAgenda"
                })
    except Exception as e: logger.error(f"Erreur OpenAgenda: {e}")
    return deals

# --- ENVOI TELEGRAM ---
def envoyer_telegram(deal):
    emojis = {"Alim'confiance": "🍴", "DATAtourisme": "🎡", "OpenAgenda": "📅"}
    emoji = emojis.get(deal['source'], "✨")
    
    caption = (
        f"{emoji} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n\n"
        f"🔗 [Voir les détails]({deal['url']})"
    )
    
    method = "sendPhoto" if deal.get("image") else "sendMessage"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    payload = {"chat_id": CHAT_ID, "parse_mode": "Markdown"}
    
    if deal.get("image"):
        payload["photo"] = deal["image"]
        payload["caption"] = caption
    else:
        payload["text"] = caption
        
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e: logger.error(f"Erreur Telegram: {e}")

# --- JOB PRINCIPAL ---
def job():
    logger.info("Lancement du Scan Cezap...")
    init_db()
    # On cumule les 3 sources !
    alertes = get_alim_confiance() + get_datatourisme() + get_open_agenda()
    
    envois = 0
    for a in alertes:
        if is_new(a["id"], "Test_CE"):
            envoyer_telegram(a)
            save_alert(a["id"], "Test_CE")
            envois += 1
            time.sleep(3)
            
    logger.info(f"Scan terminé. {envois} alertes envoyées.")

if __name__ == "__main__":
    if TELEGRAM_TOKEN and CHAT_ID:
        job()
        schedule.every(4).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
