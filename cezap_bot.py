import os
import requests
import logging
from datetime import datetime

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OA_KEY = "530b3702404e46098a2c55486f0265f4"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AGENDAS = [
    ("84693279", "Paris Culture"),
    ("55980392", "Que faire a Paris"),
    ("40839291", "Ile-de-France"),
    ("86184123", "Sciences IDF"),
    ("81082412", "Exploradome"),
]

def get_events(agenda_uid, ville):
    deals = []
    try:
        url = f"https://api.openagenda.com/v2/agendas/{agenda_uid}/events"
        params = {
            "key": OA_KEY,
            "size": 10,
            "lang": "fr",
            "sort": "updatedAt.desc"
        }
        resp = requests.get(url, params=params, timeout=15)
        logger.info(f"OpenAgenda {ville} ({agenda_uid}) -> {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"Erreur : {resp.text[:200]}")
            return []
        data = resp.json()
        events = data.get("events", [])
        logger.info(f"OpenAgenda {ville} -> {len(events)} evenement(s)")
        for e in events:
            try:
                titre = e.get("title", {}).get("fr") or e.get("title", {}).get("en", "")
                if not titre or len(titre) < 3:
                    continue
                lieu = e.get("location", {}).get("name", ville)
                city = e.get("location", {}).get("city", "")
                if city:
                    lieu = f"{lieu}, {city}"
                desc_raw = e.get("description", {})
                description = desc_raw.get("fr", "")[:150] if isinstance(desc_raw, dict) else ""
                image = None
                img = e.get("image", {})
                if isinstance(img, dict):
                    image = img.get("base") or img.get("filename_url")
                slug = e.get("slug", "")
                uid = e.get("uid", "")
                deals.append({
                    "id": f"oa_{agenda_uid}_{uid}",
                    "titre": titre,
                    "lieu": lieu or ville,
                    "description": description,
                    "image": image,
                    "url": f"https://openagenda.com/agendas/{agenda_uid}/events/{slug}" if slug else "https://openagenda.com",
                    "source": ville
                })
            except Exception as ex:
                logger.error(f"Erreur event : {ex}")
                continue
    except Exception as e:
        logger.error(f"Erreur agenda {ville}: {e}")
    return deals

def envoyer_telegram(deal):
    description = deal.get("description", "")
    desc_str = f"\n💬 _{description[:120]}..._\n" if description and len(description) > 10 else "\n"
    caption = (
        f"📅 *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"{desc_str}\n"
        f"🔗 [Voir les détails]({deal['url']})"
    )
    method = "sendPhoto" if deal.get("image") else "sendMessage"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    payload = {"chat_id": CHAT_ID, "parse_mode": "Markdown", "disable_web_page_preview": False}
    if deal.get("image"):
        payload["photo"] = deal["image"]
        payload["caption"] = caption
    else:
        payload["text"] = caption
    resp = requests.post(url, json=payload, timeout=10)
    logger.info(f"Telegram {resp.status_code} — {deal['titre']}")

if __name__ == "__main__":
    logger.info("Test OpenAgenda API v2...")
    total = 0
    for uid, ville in AGENDAS:
        events = get_events(uid, ville)
        for e in events[:3]:
            envoyer_telegram(e)
            total += 1
    logger.info(f"Test termine — {total} alertes envoyees")
