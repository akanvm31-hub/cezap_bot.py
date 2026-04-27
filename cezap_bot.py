import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
from datetime import datetime

# --- CONFIGURATION ---
# Ton lien DATAtourisme complet avec ta clé
DATATOURISME_FLUX_URL = "https://diffuseur.datatourisme.fr/webservice/f5ba593fdfeb0297cc2f33aed8fb203f/f23e25c9-de75-481c-be0c-76beac417ade" 

# Variables Railway (à configurer dans l'onglet "Variables" de Railway)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

DB_NAME = "cezap.db"

# Logging pour le suivi sur Railway
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
    params = {
        "dataset": "export_alimconfiance",
        "q": ville,
        "refine.region": "ÎLE-DE-FRANCE",
        "rows": 5,
        "refine.synthese_eval_sanitaire": "Très satisfaisant"
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            for r in resp.json().get("records", []):
                f = r.get("fields", {})
                nom = f.get("app_libelle_etablissement")
                commune = f.get("libelle_commune", "")
                
                deal_id = "alim_" + hashlib.md5(f"{nom}{commune}".encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": f"{f.get('adresse_2_ua', '')} {commune}",
                    "categorie": "Restaurant (Hygiène Top ★★★)",
                    "image": None,
                    "url": f"https://www.google.com/search?q={nom}+{commune}",
                    "source": "Alim'confiance"
                })
    except Exception as e: logger.error(f"Erreur Alim: {e}")
    return deals

# --- SOURCE 2 : LOISIRS (DATAtourisme) ---
def get_datatourisme():
    deals = []
    try:
        resp = requests.get(DATATOURISME_FLUX_URL, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("@graph", [])
            for item in items:
                nom = item.get("rdfs:label", {}).get("@value")
                if not nom: continue
                
                # Récupération image
                img_url = None
                if "hasMainRepresentation" in item:
                    try:
                        img_url = item["hasMainRepresentation"]["ebucore:hasRelatedResource"]["ebucore:locate"]["@id"]
                    except: pass

                deal_id = "data_" + hashlib.md5(nom.encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": item.get("isLocatedAt", [{}]).get("schema:address", [{}]).get("schema:addressLocality", "IDF"),
                    "categorie": "Sortie & Loisirs",
                    "image": img_url,
                    "url": item.get("@id", "https://www.datatourisme.fr"),
                    "source": "DATAtourisme"
                })
    except Exception as e: logger.error(f"Erreur DATAtourisme: {e}")
    return deals

# --- ENVOI TELEGRAM ---
def envoyer_telegram(deal):
    emoji = "🍴" if deal['source'] == "Alim'confiance" else "🎡"
    caption = (
        f"{emoji} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n\n"
        f"🔗 [Voir les détails]({deal['url']})"
    )
    
    if deal.get("image"):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        payload = {"chat_id": CHAT_ID, "photo": deal["image"], "caption": caption, "parse_mode": "Markdown"}
    else:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": caption, "parse_mode": "Markdown"}
        
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e: logger.error(f"Erreur Telegram: {e}")

# --- JOB PRINCIPAL ---
def job():
    logger.info("Lancement du Scan Cezap...")
    init_db()
    alertes = get_alim_confiance() + get_datatourisme()
    
    envois = 0
    for a in alertes:
        if is_new(a["id"], "Test_CE"):
            envoyer_telegram(a)
            save_alert(a["id"], "Test_CE")
            envois += 1
            time.sleep(3) # Anti-spam
            
    logger.info(f"Scan terminé. {envois} alertes envoyées.")

if __name__ == "__main__":
    if TELEGRAM_TOKEN and CHAT_ID:
        job() # Premier scan au démarrage
        schedule.every(4).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        logger.error("Variables TELEGRAM_TOKEN ou CHAT_ID manquantes !")
