import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
from datetime import datetime

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
# On change encore le nom pour repartir sur une base propre
DB_NAME = "cezap_ultra_safe.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RECHERCHES = [
    {"ville": "Paris", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Paris", "type": "tourist_attraction", "categorie": "Sortie Culturelle", "emoji": "📸"},
    {"ville": "Paris", "type": "museum", "categorie": "Musée", "emoji": "🏛️"},
    {"ville": "Paris", "type": "movie_theater", "categorie": "Cinéma", "emoji": "🎬"}
]

VILLES_COORDS = {"Paris": "48.8566,2.3522"}

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS sent_alerts (item_id TEXT PRIMARY KEY, client TEXT, date_sent TEXT)")
    conn.commit()
    conn.close()

def is_new(item_id):
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM sent_alerts WHERE item_id = ?", (item_id,)).fetchone()
    conn.close()
    return res is None

def save_alert(item_id):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR IGNORE INTO sent_alerts VALUES (?, ?, ?)", (item_id, "Prod_CE", datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_google_places(ville, place_type, categorie, emoji):
    deals = []
    logger.info(f"🔎 Scan Google : {categorie} à {ville}...")
    try:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": VILLES_COORDS.get(ville),
            "radius": 10000,
            "type": place_type,
            "key": GOOGLE_API_KEY,
            "language": "fr"
        }
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()

        if not isinstance(data, dict): return []

        results = data.get("results", [])
        if not isinstance(results, list): return []

        for place in results[:10]: # On prend les 10 premiers
            try:
                # Sécurité : on vérifie que 'place' est bien un dictionnaire
                if not isinstance(place, dict): continue

                nom = place.get("name")
                place_id = place.get("place_id")
                if not nom or not place_id: continue

                note = place.get("rating", 0)
                if note < 3.0: continue

                # Extraction ultra-sécurisée de la photo
                image_url = None
                photos = place.get("photos")
                if isinstance(photos, list) and len(photos) > 0:
                    first_photo = photos
                    if isinstance(first_photo, dict):
                        photo_ref = first_photo.get("photo_reference")
                        if photo_ref:
                            image_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=600&photo_reference={photo_ref}&key={GOOGLE_API_KEY}"

                deals.append({
                    "id": f"gp_{place_id}",
                    "titre": nom,
                    "lieu": place.get("vicinity", ville),
                    "categorie": categorie,
                    "emoji": emoji,
                    "note": note,
                    "avis": place.get("user_ratings_total", 0),
                    "image": image_url,
                    "url": f"https://www.google.com/maps/search/?api=1&query={nom.replace(' ', '+')}&query_place_id={place_id}"
                })
            except Exception as e_item:
                logger.error(f"⚠️ Erreur sur un item : {e_item}")
                continue

    except Exception as e:
        logger.error(f"💥 Erreur API Google : {e}")
    return deals

def envoyer_telegram(deal):
    texte = (
        f"{deal['emoji']} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n"
        f"⭐ {deal['note']}/5 ({deal['avis']} avis)\n\n"
        f"🔗 [Voir sur Google Maps]({deal['url']})"
    )

    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"
    try:
        if deal['image']:
            requests.post(base_url + "sendPhoto", data={"chat_id": CHAT_ID, "photo": deal['image'], "caption": texte, "parse_mode": "Markdown"}, timeout=10)
        else:
            requests.post(base_url + "sendMessage", data={"chat_id": CHAT_ID, "text": texte, "parse_mode": "Markdown"}, timeout=10)
        logger.info(f"🚀 Succès : {deal['titre']}")
    except Exception as e:
        logger.error(f"💥 Erreur Telegram : {e}")

def job():
    logger.info("--- DÉMARRAGE SCAN ---")
    init_db()
    total = 0
    for r in RECHERCHES:
        deals = get_google_places(r["ville"], r["type"], r["categorie"], r["emoji"])
        for d in deals:
            if is_new(d["id"]):
                envoyer_telegram(d)
                save_alert(d["id"])
                total += 1
                time.sleep(1)
    logger.info(f"--- FIN : {total} messages envoyés ---")

if __name__ == "__main__":
    if not GOOGLE_API_KEY:
        logger.error("❌ Manque la clé Google")
    else:
        job()
        schedule.every(6).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
