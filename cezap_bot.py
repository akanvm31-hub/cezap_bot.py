import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
import feedparser  # Assure-toi de l'avoir dans ton requirements.txt
from datetime import datetime
from bs4 import BeautifulSoup

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

# --- SOURCE 1 : SORTIR À PARIS (RSS - TRÈS STABLE) ---
def get_sortiraparis():
    deals = []
    rss_url = "https://www.sortiraparis.com/rss.xml"
    try:
        feed = feedparser.parse(rss_url)
        logger.info(f"Sortir à Paris : {len(feed.entries)} articles")
        for entry in feed.entries[:10]:
            deal_id = "sap_" + hashlib.md5(entry.link.encode()).hexdigest()[:12]
            # Extraction image si dispo
            image = None
            if 'media_content' in entry:
                image = entry.media_content['url']
            
            deals.append({
                "id": deal_id,
                "titre": entry.title,
                "lieu": "Île-de-France",
                "categorie": "🌟 Culture & Sorties",
                "image": image,
                "url": entry.link,
                "source": "Sortir à Paris",
                "description": entry.summary[:150] + "..." if 'summary' in entry else ""
            })
    except Exception as e:
        logger.error(f"Erreur RSS Sortir à Paris: {e}")
    return deals

# --- SOURCE 2 : BILLETREDUC (PROMOS) ---
def get_billetreduc(ville="Paris"):
    deals = []
    try:
        url = f"https://www.billetreduc.com/paris/liste.htm"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Ciblage plus robuste
            cards = soup.select(".lelt") or soup.select("table[class*='lelt']")
            for card in cards[:6]:
                try:
                    link_el = card.find("a", class_="bold")
                    if not link_el: continue
                    titre = link_el.get_text(strip=True)
                    url_deal = "https://www.billetreduc.com" + link_el['href']
                    
                    deal_id = "br_" + hashlib.md5(url_deal.encode()).hexdigest()[:12]
                    deals.append({
                        "id": deal_id, "titre": titre, "lieu": ville,
                        "categorie": "🎟️ Promo Spectacle", "image": None,
                        "url": url_deal, "source": "BilletReduc", 
                        "description": "Profitez de tarifs réduits sur BilletReduc."
                    })
                except: continue
    except Exception as e:
        logger.error(f"Erreur BilletReduc: {e}")
    return deals

# --- SOURCE 3 : QUE FAIRE À PARIS (OPEN DATA) ---
def get_paris_events():
    deals = []
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {"dataset": "que-faire-a-paris-", "rows": 5, "sort": "date_start"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            for r in resp.json().get("records", []):
                f = r.get("fields", {})
                deal_id = "paris_" + r.get("recordid", "")[:12]
                deals.append({
                    "id": deal_id, "titre": f.get("title", ""), 
                    "lieu": f"Paris ({f.get('address_zipcode', '')})",
                    "categorie": f"🎭 {f.get('category', 'Événement')}",
                    "image": f.get("cover_url"),
                    "url": f.get("url", "https://quefaire.paris.fr"),
                    "source": "Que faire à Paris",
                    "description": f.get("lead_text", "")[:150]
                })
    except Exception as e:
        logger.error(f"Erreur Paris Events: {e}")
    return deals

# --- ENVOI TELEGRAM ---
def envoyer_telegram(deal):
    emojis = {"Sortir à Paris": "🌟", "BilletReduc": "🎟️", "Que faire à Paris": "🎭"}
    emoji = emojis.get(deal["source"], "✨")
    
    caption = (
        f"{emoji} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n\n"
        f"💬 _{deal['description']}_\n\n"
        f"🔗 [Voir les détails]({deal['url']})"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"
    method = "sendPhoto" if deal.get("image") else "sendMessage"
    payload = {"chat_id": CHAT_ID, "parse_mode": "Markdown"}
    
    if deal.get("image"):
        payload["photo"] = deal["image"]
        payload["caption"] = caption
    else:
        payload["text"] = caption

    try:
        requests.post(url + method, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Erreur envoi Telegram: {e}")

# --- JOB PRINCIPAL ---
def job(avec_resume=False):
    logger.info("Scan en cours...")
    init_db()
    
    # On rassemble toutes les sources
    alertes = get_sortiraparis() + get_paris_events() + get_billetreduc()
    
    envois = 0
    for a in alertes:
        if is_new(a["id"], "Prod_CE"):
            envoyer_telegram(a)
            save_alert(a["id"], "Prod_CE")
            envois += 1
            time.sleep(2)
    logger.info(f"Scan terminé. {envois} nouvelles alertes.")

# --- LANCEMENT ---
if __name__ == "__main__":
    if TELEGRAM_TOKEN and CHAT_ID:
        job() # Premier lancement
        schedule.every(4).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        logger.error("Variables d'environnement manquantes !")
