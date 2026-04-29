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
# On change le nom ici pour forcer le bot à tout renvoyer (Reset du cache)
DB_NAME = "cezap_reset_final.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RECHERCHES = [
    {"ville": "Paris", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Paris", "type": "tourist_attraction", "categorie": "Lieu Touristique", "emoji": "📸"},
    {"ville": "Paris", "type": "museum", "categorie": "Musée", "emoji": "🏛️"},
    {"ville": "Paris", "type": "movie_theater", "categorie": "Cinéma", "emoji": "🎬"}
]

VILLES_COORDS = {"Paris": "48.8566,2.3522"}

# --- BASE DE DONNÉES ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS sent_alerts (item_id TEXT PRIMARY KEY, client TEXT, date_sent TEXT)")
    conn.commit()
    conn.close()

def is_new(item_id, client_nom):
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute("SELECT 1 FROM sent_alerts WHERE item_id = ? AND client = ?", (item_id, client_nom)).fetchone()
    conn.close()
    return res is None

def save_alert(item_id, client_nom):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR IGNORE INTO sent_alerts VALUES (?, ?, ?)", (item_id, client_nom, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# --- LOGIQUE GOOGLE PLACES ---
def get_google_places(ville, place_type, categorie, emoji):
    deals = []
    logger.info(f"🔎 Scan Google : {categorie} à {ville}...")
    try:
        coords = VILLES_COORDS.get(ville, "48.8566,2.3522")
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": coords,
            "radius": 10000,
            "type": place_type,
            "key": GOOGLE_API_KEY,
            "language": "fr"
        }
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()

        if not isinstance(data, dict):
            logger.error("❌ Erreur format JSON")
            return []

        results = data.get("results", [])
        logger.info(f"✅ {len(results)} résultats reçus pour {categorie}")

        for place in results[:5]:
            note = place.get("rating", 0)
            nb_avis = place.get("user_ratings_total", 0)
            nom = place.get("name")

            # FILTRE : On accepte à partir de 3.0 pour être sûr d'avoir du contenu
            if note < 3.0:
                continue

            place_id = place.get("place_id")
            if not place_id: continue

            # Gestion de la photo
            image_url = None
            photos = place.get("photos")
            if photos and isinstance(photos, list) and len(photos) > 0:
                photo_ref = photos.get("photo_reference")
                if photo_ref:
                    image_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=600&photo_reference={photo_ref}&key={GOOGLE_API_KEY}"

            deals.append({
                "id": "gp_" + hashlib.md5(place_id.encode()).hexdigest()[:12],
                "titre": nom,
                "lieu": place.get("vicinity"),
                "categorie": categorie,
                "emoji": emoji,
                "note": note,
                "avis": nb_avis,
                "image": image_url,
                "url": f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={place_id}"
            })
    except Exception as e:
        logger.error(f"💥 Erreur Google : {e}")
    return deals

# --- ENVOI TELEGRAM ---
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
            # Utilisation de 'data' au lieu de 'json' pour les photos (plus stable)
            requests.post(base_url + "sendPhoto", data={"chat_id": CHAT_ID, "photo": deal['image'], "caption": texte, "parse_mode": "Markdown"}, timeout=10)
        else:
            requests.post(base_url + "sendMessage", data={"chat_id": CHAT_ID, "text": texte, "parse_mode": "Markdown"}, timeout=10)
        logger.info(f"🚀 Succès : {deal['titre']}")
    except Exception as e:
        logger.error(f"💥 Erreur envoi : {e}")

# --- JOB ---
def job():
    logger.info("--- DÉMARRAGE DU SCAN FINAL ---")
    init_db()
    total = 0
    for r in RECHERCHES:
        deals = get_google_places(r["ville"], r["type"], r["categorie"], r["emoji"])
        for d in deals:
            # Cette condition va maintenant être True pour tout car la DB est nouvelle
            if is_new(d["id"], "Prod_CE"):
                envoyer_telegram(d)
                save_alert(d["id"], "Prod_CE")
                total += 1
                time.sleep(1.5) # On ralentit un peu pour Telegram
    logger.info(f"--- FIN : {total} messages envoyés ---")

if __name__ == "__main__":
    if not all([TELEGRAM_TOKEN, CHAT_ID, GOOGLE_API_KEY]):
        logger.error("❌ Variables manquantes sur Railway !")
    else:
        job()
        schedule.every(8).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
