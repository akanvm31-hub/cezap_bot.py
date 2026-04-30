import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
from datetime import datetime

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
DB_NAME = "cezap_production_v1.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RECHERCHES = [
    {"ville": "Paris", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Paris", "type": "tourist_attraction", "categorie": "Sortie Culturelle", "emoji": "📸"},
    {"ville": "Paris", "type": "museum", "categorie": "Musee", "emoji": "🏛️"},
    {"ville": "Paris", "type": "movie_theater", "categorie": "Cinema", "emoji": "🎬"},
    {"ville": "Paris", "type": "spa", "categorie": "Bien-etre & Spa", "emoji": "💆"},
    {"ville": "Paris", "type": "amusement_park", "categorie": "Parc d'attractions", "emoji": "🎡"},
    {"ville": "Paris", "type": "bowling_alley", "categorie": "Bowling", "emoji": "🎳"},
    {"ville": "Paris", "type": "zoo", "categorie": "Zoo", "emoji": "🦁"},
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

def get_place_details(place_id):
    """Recupere le site web officiel et numero de telephone"""
    try:
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "fields": "website,formatted_phone_number,opening_hours",
            "key": GOOGLE_API_KEY,
            "language": "fr"
        }
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        result = data.get("result", {})
        return {
            "website": result.get("website"),
            "phone": result.get("formatted_phone_number"),
            "open_now": result.get("opening_hours", {}).get("open_now")
        }
    except:
        return {}

def get_google_places(ville, place_type, categorie, emoji):
    deals = []
    logger.info(f"Scan Google : {categorie} a {ville}...")
    try:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": VILLES_COORDS.get(ville),
            "radius": 10000,
            "type": place_type,
            "key": GOOGLE_API_KEY,
            "language": "fr"
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        if not isinstance(data, dict):
            return []
        results = data.get("results", [])
        if not isinstance(results, list):
            return []

        for place in results[:8]:
            try:
                if not isinstance(place, dict):
                    continue
                nom = place.get("name")
                place_id = place.get("place_id")
                if not nom or not place_id:
                    continue
                note = place.get("rating", 0)
                if note < 3.5:
                    continue

                # Photo
                image_url = None
                photos = place.get("photos")
                if isinstance(photos, list) and len(photos) > 0:
                    photo_ref = photos[0].get("photo_reference")
                    if photo_ref:
                        image_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photo_reference={photo_ref}&key={GOOGLE_API_KEY}"

                # Recuperer site officiel
                details = get_place_details(place_id)
                website = details.get("website")
                phone = details.get("phone")
                open_now = details.get("open_now")

                # Lien direct vers site officiel sinon Google Maps
                google_maps_link = f"https://www.google.com/maps/search/?api=1&query={nom.replace(' ', '+')}&query_place_id={place_id}"

                deals.append({
                    "id": f"gp_{place_id}",
                    "titre": nom,
                    "lieu": place.get("vicinity", ville),
                    "categorie": categorie,
                    "emoji": emoji,
                    "note": note,
                    "avis": place.get("user_ratings_total", 0),
                    "image": image_url,
                    "website": website,
                    "phone": phone,
                    "open_now": open_now,
                    "google_maps": google_maps_link
                })
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Erreur API : {e}")
    return deals

def envoyer_telegram(deal):
    open_status = ""
    if deal.get("open_now") is True:
        open_status = "🟢 Ouvert maintenant\n"
    elif deal.get("open_now") is False:
        open_status = "🔴 Ferme maintenant\n"

    phone_line = f"📞 {deal['phone']}\n" if deal.get("phone") else ""

    # Lien principal : site officiel si dispo
    if deal.get("website"):
        lien_principal = f"🎟️ [Reserver sur le site officiel]({deal['website']})"
        lien_secondaire = f"\n📍 [Voir sur Google Maps]({deal['google_maps']})"
    else:
        lien_principal = f"📍 [Voir sur Google Maps]({deal['google_maps']})"
        lien_secondaire = ""

    texte = (
        f"{deal['emoji']} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n"
        f"⭐ {deal['note']}/5 ({deal['avis']} avis Google)\n"
        f"{open_status}"
        f"{phone_line}\n"
        f"{lien_principal}"
        f"{lien_secondaire}"
    )

    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"
    try:
        if deal['image']:
            requests.post(base_url + "sendPhoto", data={"chat_id": CHAT_ID, "photo": deal['image'], "caption": texte, "parse_mode": "Markdown"}, timeout=15)
        else:
            requests.post(base_url + "sendMessage", data={"chat_id": CHAT_ID, "text": texte, "parse_mode": "Markdown"}, timeout=15)
        logger.info(f"Envoye : {deal['titre']}")
    except Exception as e:
        logger.error(f"Erreur Telegram : {e}")

def job():
    logger.info("--- DEBUT DU SCAN CEZAP ---")
    init_db()
    total = 0
    for r in RECHERCHES:
        deals = get_google_places(r["ville"], r["type"], r["categorie"], r["emoji"])
        for d in deals:
            if is_new(d["id"]):
                envoyer_telegram(d)
                save_alert(d["id"])
                total += 1
                time.sleep(2)
    logger.info(f"--- FIN DU SCAN : {total} nouveaux messages ---")

if __name__ == "__main__":
    if not all([TELEGRAM_TOKEN, CHAT_ID, GOOGLE_API_KEY]):
        logger.error("Variables d'environnement manquantes !")
    else:
        job()
        schedule.every(8).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
