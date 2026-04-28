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
DB_NAME = "cezap.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RECHERCHES = [
    {"ville": "Paris", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Lyon", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Marseille", "type": "restaurant", "categorie": "Restaurant", "emoji": "🍴"},
    {"ville": "Paris", "type": "amusement_park", "categorie": "Parc d'attractions", "emoji": "🎡"},
    {"ville": "Paris", "type": "bowling_alley", "categorie": "Bowling", "emoji": "🎳"},
    {"ville": "Paris", "type": "movie_theater", "categorie": "Cinema", "emoji": "🎬"},
    {"ville": "Paris", "type": "museum", "categorie": "Musee", "emoji": "🏛️"},
    {"ville": "Paris", "type": "art_gallery", "categorie": "Galerie d'art", "emoji": "🎨"},
    {"ville": "Paris", "type": "performing_arts_theater", "categorie": "Theatre", "emoji": "🎭"},
    {"ville": "Paris", "type": "spa", "categorie": "Spa et Bien-etre", "emoji": "💆"},
    {"ville": "Paris", "type": "gym", "categorie": "Sport", "emoji": "🏋️"},
    {"ville": "Paris", "type": "zoo", "categorie": "Zoo", "emoji": "🦁"},
    {"ville": "Paris", "type": "aquarium", "categorie": "Aquarium", "emoji": "🐠"},
]

VILLES_COORDS = {
    "Paris": "48.8566,2.3522",
    "Lyon": "45.7640,4.8357",
    "Marseille": "43.2965,5.3698",
}

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

def vider_cache():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("DELETE FROM sent_alerts")
    conn.commit()
    conn.close()
    logger.info("Cache vide !")

def get_google_places(ville, place_type, categorie, emoji):
    deals = []
    try:
        coords = VILLES_COORDS.get(ville, "48.8566,2.3522")
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": coords,
            "radius": 5000,
            "type": place_type,
            "key": GOOGLE_API_KEY,
            "language": "fr",
            "rankby": "prominence"
        }
        resp = requests.get(url, params=params, timeout=15)
        logger.info(f"Google Places {ville} {place_type} -> {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Erreur: {resp.text[:200]}")
            return []
        results = resp.json().get("results", [])
        logger.info(f"Google Places {ville} {place_type} -> {len(results)} resultat(s)")
        for place in results[:5]:
            try:
                nom = place.get("name", "")
                if not nom:
                    continue
                note = place.get("rating", 0)
                nb_avis = place.get("user_ratings_total", 0)
                if note < 4.0 or nb_avis < 50:
                    continue
                adresse = place.get("vicinity", ville)
                place_id = place.get("place_id", "")
                photo_ref = None
                photos = place.get("photos", [])
                if photos:
                    photo_ref = photos[0].get("photo_reference")
                image_url = None
                if photo_ref:
                    image_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photo_reference={photo_ref}&key={GOOGLE_API_KEY}"
                url_maps = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                deal_id = "gp_" + hashlib.md5(place_id.encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": adresse,
                    "categorie": categorie,
                    "emoji": emoji,
                    "note": note,
                    "avis": nb_avis,
                    "image": image_url,
                    "url": url_maps,
                })
            except Exception as e:
                logger.error(f"Erreur place: {e}")
                continue
    except Exception as e:
        logger.error(f"Erreur Google Places {ville} {place_type}: {e}")
    return deals

def envoyer_telegram(deal):
    emoji = deal.get("emoji", "✨")
    note = deal.get("note", "")
    avis = deal.get("avis", "")
    texte = (
        f"{emoji} PROPOSITION CEZAP\n\n"
        f"🎯 {deal['titre']}\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n"
        f"⭐ {note}/5 ({avis} avis Google)\n\n"
        f"🔗 {deal['url']}"
    )
    if deal.get("image"):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                json={"chat_id": CHAT_ID, "photo": deal["image"], "caption": texte},
                timeout=10
            )
            if resp.status_code == 200:
                logger.info(f"Envoye avec photo : {deal['titre']}")
                return
        except:
            pass
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": texte},
            timeout=10
        )
        logger.info(f"Envoye : {deal['titre']} ({resp.status_code})")
    except Exception as e:
        logger.error(f"Erreur Telegram: {e}")

def envoyer_resume(alertes):
    if not alertes:
        return
    top = alertes[:5]
    lignes = []
    for i, a in enumerate(top, 1):
        lignes.append(f"{i}. {a['titre']} - {a['lieu']} ({a['categorie']})")
    message = (
        f"🌅 Bonjour ! Les meilleures idees du jour pour votre CE\n\n"
        f"📅 {datetime.now().strftime('%A %d %B %Y')}\n\n"
        + "\n".join(lignes) +
        f"\n\nCezap - Votre CE toujours inspire 🎉"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": message},
            timeout=10
        )
        logger.info("Resume envoye !")
    except Exception as e:
        logger.error(f"Erreur resume: {e}")

def job(avec_resume=False):
    logger.info("Lancement scan Cezap Google Places...")
    init_db()
    if avec_resume:
        vider_cache()
    alertes = []
    for r in RECHERCHES:
        deals = get_google_places(r["ville"], r["type"], r["categorie"], r["emoji"])
        alertes.extend(deals)
        time.sleep(0.5)
    logger.info(f"Total lieux trouves : {len(alertes)}")
    if avec_resume:
        envoyer_resume(alertes)
    envois = 0
    for a in alertes:
        if is_new(a["id"], "Prod_CE"):
            envoyer_telegram(a)
            save_alert(a["id"], "Prod_CE")
            envois += 1
            time.sleep(2)
    logger.info(f"Scan termine. {envois} alertes envoyees.")

if __name__ == "__main__":
    if TELEGRAM_TOKEN and CHAT_ID:
        job(avec_resume=False)
        schedule.every().day.at("08:00").do(lambda: job(avec_resume=True))
        schedule.every(4).hours.do(job)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        logger.error("TELEGRAM_TOKEN ou CHAT_ID manquant !")
