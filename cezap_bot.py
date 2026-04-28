import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
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

# --- SOURCE 1 : RESTAURATION (Alim'confiance) ---
def get_alim_confiance(ville="Paris"):
    deals = []
    url = "https://dgal.opendatasoft.com/api/records/1.0/search/"
    params = {
        "dataset": "export_alimconfiance",
        "q": ville,
        "rows": 8,
        "refine.synthese_eval_sanitaire": "Très satisfaisant"
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            for r in resp.json().get("records", []):
                f = r.get("fields", {})
                nom = f.get("app_libelle_etablissement", "").strip()
                if not nom or len(nom) < 3 or nom.upper() in ["PARIS", "FRANCE", "IDF"]:
                    continue
                adresse = f.get("adresse_etablissement", "")
                commune = f.get("libelle_commune", "")
                lieu = f"{adresse}, {commune}".strip(", ")
                deal_id = "alim_" + hashlib.md5(f"{nom}{adresse}".encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom.title(),
                    "lieu": lieu,
                    "categorie": "🍽️ Restaurant — Hygiène certifiée ★★★",
                    "image": None,
                    "url": f"https://www.google.com/search?q={nom.replace(' ', '+')}+{commune}+restaurant",
                    "source": "Alim'confiance",
                    "description": "Établissement avec une note hygiène TRÈS SATISFAISANTE selon les contrôles officiels."
                })
    except Exception as e:
        logger.error(f"Erreur Alim: {e}")
    return deals

# --- SOURCE 2 : LOISIRS (DATAtourisme) ---
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
                localisation = "Île-de-France"
                is_located = item.get("isLocatedAt", [])
                if isinstance(is_located, list) and is_located:
                    address = is_located[0].get("schema:address", {})
                    if isinstance(address, list) and address:
                        address = address[0]
                    if isinstance(address, dict):
                        localisation = address.get("schema:addressLocality", "Île-de-France")
                elif isinstance(is_located, dict):
                    address = is_located.get("schema:address", {})
                    if isinstance(address, dict):
                        localisation = address.get("schema:addressLocality", "Île-de-France")
                deal_id = "data_" + hashlib.md5(nom.encode()).hexdigest()[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": localisation,
                    "categorie": "🎡 Sortie & Loisirs",
                    "image": None,
                    "url": item.get("@id", "https://www.datatourisme.fr"),
                    "source": "DATAtourisme",
                    "description": "Lieu de loisirs référencé par le Ministère du Tourisme français."
                })
    except Exception as e:
        logger.error(f"Erreur DATAtourisme: {e}")
    return deals

# --- SOURCE 3 : EVENEMENTS (OpenAgenda) ---
def get_open_agenda():
    deals = []
    url = "https://openagenda.com/agendas/89803504/events.json"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            events = resp.json().get("events", [])
            for e in events[:8]:
                titre = e.get("title", {})
                nom = titre.get("fr") or titre.get("en") if isinstance(titre, dict) else str(titre)
                if not nom or len(nom) < 3:
                    continue
                lieu = e.get("locationName", "Île-de-France")
                if isinstance(lieu, dict):
                    lieu = lieu.get("fr", "Île-de-France")
                deal_id = "open_" + str(e.get("uid", hashlib.md5(nom.encode()).hexdigest()[:8]))
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": lieu,
                    "categorie": "📅 Événement / Expo",
                    "image": e.get("image"),
                    "url": f"https://openagenda.com/event/{e.get('slug', '')}",
                    "source": "OpenAgenda",
                    "description": e.get("description", {}).get("fr", "") if isinstance(e.get("description"), dict) else ""
                })
    except Exception as e:
        logger.error(f"Erreur OpenAgenda: {e}")
    return deals

# --- SOURCE 4 : SPECTACLES (Que faire à Paris ?) ---
def get_paris_events():
    deals = []
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {
        "dataset": "que-faire-a-paris-",
        "rows": 8,
        "sort": "date_start"
    }
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
                date_str = f" — {date_debut[:10]}" if date_debut else ""
                deal_id = "paris_" + r.get("recordid", "")[:12]
                deals.append({
                    "id": deal_id,
                    "titre": nom,
                    "lieu": lieu,
                    "categorie": f"🎭 {categorie_raw}{date_str}",
                    "image": f.get("cover_url"),
                    "url": f.get("url", "https://quefaire.paris.fr"),
                    "source": "Que faire à Paris",
                    "description": f.get("lead_text", "")
                })
    except Exception as e:
        logger.error(f"Erreur Paris Events: {e}")
    return deals

# --- SOURCE 5 : BILLETREDUC ---
def get_billetreduc(ville="Paris"):
    deals = []
    try:
        url = f"https://www.billetreduc.com/rech.htm?ville={ville}"
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
            "Accept-Language": "fr-FR,fr;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=20)
        logger.info(f"BilletReduc status : {resp.status_code}")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("div[class*='event']") or \
                    soup.select("article") or \
                    soup.select("div[class*='spectacle']") or \
                    soup.select("li[class*='event']")
            logger.info(f"BilletReduc : {len(cards)} spectacle(s)")
            for card in cards[:8]:
                try:
                    titre_el = card.select_one("h2") or card.select_one("h3") or card.select_one("[class*='title']")
                    titre = titre_el.get_text(strip=True) if titre_el else ""
                    if not titre or len(titre) < 3:
                        continue
                    href = (card.select_one("a") or {}).get("href", "")
                    url_deal = href if href.startswith("http") else "https://www.billetreduc.com" + href
                    prix_el = card.select_one("[class*='price']") or card.select_one("[class*='tarif']")
                    prix_txt = prix_el.get_text(strip=True) if prix_el else ""
                    prix = int("".join(filter(str.isdigit, prix_txt.split("€")[0].replace(" ", "")))) if "€" in prix_txt else 0
                    reduction_el = card.select_one("[class*='reduc']") or card.select_one("[class*='discount']")
                    reduction_txt = reduction_el.get_text(strip=True) if reduction_el else ""
                    reduction = int("".join(filter(str.isdigit, reduction_txt))) if any(c.isdigit() for c in reduction_txt) else 0
                    desc = f"À partir de {prix}€" if prix > 0 else ""
                    if reduction > 0:
                        desc += f" — -{reduction}% de réduction"
                    deal_id = "br_" + hashlib.md5(url_deal.encode()).hexdigest()[:12]
                    deals.append({
                        "id": deal_id,
                        "titre": titre,
                        "lieu": ville,
                        "categorie": "🎟️ Spectacle — Billet réduit",
                        "image": None,
                        "url": url_deal,
                        "source": "BilletReduc",
                        "description": desc
                    })
                except:
                    continue
    except Exception as e:
        logger.error(f"Erreur BilletReduc: {e}")
    return deals

# --- ENVOI TELEGRAM ---
def envoyer_telegram(deal):
    emojis = {
        "Alim'confiance": "🍴",
        "DATAtourisme": "🎡",
        "OpenAgenda": "📅",
        "Que faire à Paris": "🎭",
        "BilletReduc": "🎟️"
    }
    emoji = emojis.get(deal["source"], "✨")
    description = deal.get("description", "")
    desc_str = f"\n💬 _{description[:120]}..._\n" if description and len(description) > 10 else "\n"

    caption = (
        f"{emoji} *PROPOSITION CEZAP*\n\n"
        f"🎯 *{deal['titre']}*\n"
        f"📍 {deal['lieu']}\n"
        f"📂 {deal['categorie']}\n"
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

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Envoye : {deal['titre']}")
        else:
            logger.error(f"Erreur Telegram {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        logger.error(f"Erreur Telegram: {e}")

# --- RESUME QUOTIDIEN 8H ---
def envoyer_resume(alertes):
    if not alertes:
        return
    top = alertes[:5]
    lignes = []
    for i, a in enumerate(top, 1):
        lignes.append(f"{i}. *{a['titre']}* — {a['lieu']} ({a['source']})")
    message = (
        f"🌅 *Bonjour ! Les meilleures idées du jour pour votre CE*\n\n"
        f"📅 {datetime.now().strftime('%A %d %B %Y')}\n\n"
        + "\n".join(lignes) +
        f"\n\n_Cezap — Votre CE toujours inspiré_ 🎉"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }, timeout=10)
        logger.info("Resume quotidien envoye !")
    except Exception as e:
        logger.error(f"Erreur resume: {e}")

# --- JOB PRINCIPAL ---
def job(avec_resume=False):
    logger.info("Lancement du scan Cezap...")
    init_db()

    alertes = (
        get_alim_confiance() +
        get_datatourisme() +
        get_open_agenda() +
        get_paris_events() +
        get_billetreduc()
    )

    if avec_resume:
        envoyer_resume(alertes)

    envois = 0
    for a in alertes:
        if not a.get("titre") or len(a["titre"]) < 3:
            continue
        if is_new(a["id"], "Prod_CE"):
            envoyer_telegram(a)
            save_alert(a["id"], "Prod_CE")
            envois += 1
            time.sleep(2)

    logger.info(f"Scan termine. {envois} alertes envoyees.")

# --- MAIN ---
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
