import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
DB_NAME = "cezap.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# ══════════════════════════════════════════════
# SOURCE PRINCIPALE : OpenAgenda multi-agendas
# ══════════════════════════════════════════════
AGENDAS = [
    ("89803504", "Ile-de-France"),      # Sortir en IDF
    ("55980392", "Paris"),               # Que faire a Paris
    ("52357978", "Lyon"),                # Agenda Lyon
    ("83192274", "Marseille"),           # Agenda Marseille
    ("19291915", "Bordeaux"),            # Agenda Bordeaux
    ("74836251", "Toulouse"),            # Agenda Toulouse
    ("48984891", "Nantes"),              # Agenda Nantes
    ("29312421", "France Culture"),      # Culture France
]

def get_openagenda_multi():
    deals = []
    for agenda_id, ville in AGENDAS:
        try:
            url = f"https://openagenda.com/agendas/{agenda_id}/events.json"
            resp = requests.get(url, timeout=10)
            logger.info(f"OpenAgenda {ville} ({agenda_id}) status : {resp.status_code}")
            if resp.status_code != 200:
                continue
            events = resp.json().get("events", [])
            logger.info(f"OpenAgenda {ville} : {len(events)} evenement(s)")
            for e in events[:5]:
                try:
                    titre_raw = e.get("title", {})
                    nom = titre_raw.get("fr") or titre_raw.get("en") if isinstance(titre_raw, dict) else str(titre_raw)
                    if not nom or len(nom) < 3:
                        continue
                    lieu_raw = e.get("locationName", ville)
                    lieu = lieu_raw.get("fr", ville) if isinstance(lieu_raw, dict) else str(lieu_raw)
                    desc_raw = e.get("description", {})
                    description = ""
                    if isinstance(desc_raw, dict):
                        description = desc_raw.get("fr", "")[:150]
                    image = e.get("image")
                    slug = e.get("slug", "")
                    uid = e.get("uid", "")
                    deal_id = f"oa_{agenda_id}_{uid}"
                    deals.append({
                        "id": deal_id,
                        "titre": nom,
                        "lieu": lieu or ville,
                        "categorie": "📅 Evenement culturel",
                        "image": image,
                        "url": f"https://openagenda.com/event/{slug}" if slug else f"https://openagenda.com",
                        "source": f"OpenAgenda {ville}",
                        "description": description
                    })
                except:
                    continue
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Erreur OpenAgenda {ville}: {e}")
    return deals

# ══════════════════════════════════════════════
# SOURCE 2 : Que faire a Paris (API officielle)
# ══════════════════════════════════════════════
def get_paris_events():
    deals = []
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {"dataset": "que-faire-a-paris-", "rows": 10, "sort": "date_start"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            for r in resp.json().get("records", []):
                f = r.get("fields", {})
                nom = f.get("title", "").strip()
                if not nom or len(nom) < 3:
                    continue
                adresse = f.get("address_name", "")
                cp = f.get("address_zipcode", "")
                lieu = f"{adresse} ({cp})".strip("( )") if adresse else f"Paris {cp}"
                categorie_raw = f.get("category", "Sortie")
                date_debut = f.get("date_start", "")
                date_str = f" - {date_debut[:10]}" if date_debut else ""
                deal_id = "paris_" + r.get("recordid", "")[:12]
                deals.append({
                    "id": deal_id, "titre": nom, "lieu": lieu,
                    "categorie": f"🎭 {categorie_raw}{date_str}",
                    "image": f.get("cover_url"),
                    "url": f.get("url", "https://quefaire.paris.fr"),
                    "source": "Que faire a Paris",
                    "description": f.get("lead_text", "")
                })
    except Exception as e:
        logger.error(f"Erreur Paris Events: {e}")
    return deals

# ══════════════════════════════════════════════
# SOURCE 3 : DATAtourisme
# ══════════════════════════════════════════════
DATATOURISME_FLUX_URL = "https://diffuseur.datatourisme.fr/webservice/f5ba593fdfeb0297cc2f33aed8fb203f/f23e25c9-de75-481c-be0c-76beac417ade"

def get_datatourisme():
    deals = []
    try:
        resp = requests.get(DATATOURISME_FLUX_URL, timeout=15)
        if resp.status_code == 200:
            items = resp.json().get("@graph", [])
            for item in items[:10]:
                label = item.get("rdfs:label", {})
                if isinstance(label, dict):
                    nom = label.get("@value", "")
                elif isinstance(label, list):
                    nom = label[0].get("@value", "") if label else ""
                else:
                    nom = str(label)
                if not nom or len(nom) < 3:
                    continue
                localisation = "Ile-de-France"
                is_located = item.get("isLocatedAt", [])
                if isinstance(is_located, list) and is_located:
                    address = is_located[0].get("schema:address", {})
                    if isinstance(address, list) and address:
                        address = address[0]
                    if isinstance(address, dict):
                        localisation = address.get("schema:addressLocality", "Ile-de-France")
                elif isinstance(is_located, dict):
                    address = is_located.get("schema:address", {})
                    if isinstance(address, dict):
                        localisation = address.get("schema:addressLocality", "Ile-de-France")
                deal_id = "data_" + hashlib.md5(nom.encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id, "titre": nom, "lieu": localisation,
                    "categorie": "🎡 Sortie & Loisirs", "image": None,
                    "url": item.get("@id", "https://www.datatourisme.fr"),
                    "source": "DATAtourisme",
                    "description": "Lieu de loisirs reference par le Ministere du Tourisme."
                })
    except Exception as e:
        logger.error(f"Erreur DATAtourisme: {e}")
    return deals

# ══════════════════════════════════════════════
# ENVOI TELEGRAM
# ══════════════════════════════════════════════
def envoyer_telegram(deal):
    emoji = "📅" if "OpenAgenda" in deal["source"] else "🎭" if "Paris" in deal["source"] else "🎡"
    description = deal.get("description", "")
    desc_str = f"\n💬 _{description[:120]}..._\n" if description and len(description) > 10 else "\n"
    caption = (
        f"{emoji} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n"
        f"{desc_str}\n"
        f"🔗 [Voir les details]({deal['url']})"
    )
    method = "sendPhoto" if deal.get("image") else "sendMessage"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    payload = {"chat_id": CHAT_ID, "parse_mode": "Markdown", "disable_web_page_preview": False}
    if deal.get("image"):
        payload["photo"] = deal["image"]
        payload["caption"] = caption
    else:
        payload["text"] = caption
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Envoye : {deal['titre']}")
        else:
            logger.error(f"Erreur Telegram {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        logger.error(f"Erreur Telegram: {e}")

def envoyer_resume(alertes):
    if not alertes:
        return
    top = alertes[:5]
    lignes = []
    for i, a in enumerate(top, 1):
        lignes.append(f"{i}. *{a['titre']}* - {a['lieu']} ({a['source']})")
    message = (
        f"🌅 *Bonjour ! Les meilleures idees du jour pour votre CE*\n\n"
        f"📅 {datetime.now().strftime('%A %d %B %Y')}\n\n"
        + "\n".join(lignes) +
        f"\n\n_Cezap - Votre CE toujours inspire_ 🎉"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID, "text": message,
            "parse_mode": "Markdown", "disable_web_page_preview": True
        }, timeout=10)
        logger.info("Resume quotidien envoye !")
    except Exception as e:
        logger.error(f"Erreur resume: {e}")

# ══════════════════════════════════════════════
# JOB PRINCIPAL
# ══════════════════════════════════════════════
def job(avec_resume=False):
    logger.info("Lancement du scan Cezap...")
    init_db()
    if avec_resume:
        vider_cache()
    alertes = (
        get_openagenda_multi() +
        get_paris_events() +
        get_datatourisme()
    )
    logger.info(f"Total alertes collectees : {len(alertes)}")
    if avec_resume:
        envoyer_resume(alertes)
    envois = 0
    for a in alertes:
        if not a.get("titre") or len(a["titre"]) < 5:
            continue
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
