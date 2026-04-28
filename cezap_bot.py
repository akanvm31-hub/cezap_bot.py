import os
import requests
import hashlib
import sqlite3
import schedule
import time
import logging
import xml.etree.ElementTree as ET
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

def vider_cache():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("DELETE FROM sent_alerts")
    conn.commit()
    conn.close()
    logger.info("Cache vide !")

# --- SOURCE 1 : SORTIRAPARIS ---
def get_sortiraparis():
    deals = []
    categories = [
        ("https://www.sortiraparis.com/loisirs/spectacle", "🎭 Spectacle"),
        ("https://www.sortiraparis.com/restaurants", "🍽️ Restaurant"),
        ("https://www.sortiraparis.com/arts-culture/exposition", "🎨 Exposition"),
        ("https://www.sortiraparis.com/loisirs", "🎡 Loisirs"),
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
    }
    for url, categorie in categories:
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            logger.info(f"Sortiraparis {categorie} status : {resp.status_code}")
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select("article[class*='item']") or \
                    soup.select("div[class*='item-list']") or \
                    soup.select("div[class*='card']") or \
                    soup.select("article") or \
                    soup.select("li[class*='item']")
            logger.info(f"Sortiraparis {categorie} : {len(cards)} article(s)")
            for card in cards[:5]:
                try:
                    titre_el = card.select_one("h2") or card.select_one("h3") or \
                               card.select_one("[class*='title']") or card.select_one("a")
                    titre = titre_el.get_text(strip=True) if titre_el else ""
                    if not titre or len(titre) < 5:
                        continue
                    link_el = card.select_one("a[href*='sortiraparis']") or card.select_one("a")
                    href = link_el.get("href", "") if link_el else ""
                    url_deal = href if href.startswith("http") else "https://www.sortiraparis.com" + href
                    if not href:
                        continue
                    img_el = card.select_one("img")
                    image = img_el.get("src") or img_el.get("data-src") if img_el else None
                    if image and not image.startswith("http"):
                        image = None
                    desc_el = card.select_one("p") or card.select_one("[class*='desc']") or \
                              card.select_one("[class*='intro']")
                    description = desc_el.get_text(strip=True)[:150] if desc_el else ""
                    lieu_el = card.select_one("[class*='location']") or card.select_one("[class*='lieu']") or \
                              card.select_one("[class*='address']")
                    lieu = lieu_el.get_text(strip=True) if lieu_el else "Paris"
                    deal_id = "sap_" + hashlib.md5(url_deal.encode()).hexdigest()[:12]
                    deals.append({
                        "id": deal_id, "titre": titre, "lieu": lieu or "Paris",
                        "categorie": categorie, "image": image, "url": url_deal,
                        "source": "Sortiraparis", "description": description
                    })
                except:
                    continue
            time.sleep(1)
        except Exception as e:
            logger.error(f"Erreur Sortiraparis {categorie}: {e}")
    return deals

# --- SOURCE 2 : AGENDA CULTUREL (RSS national) ---
def get_agenda_culturel():
    deals = []
    # Flux RSS nationaux par catégorie
    flux_rss = [
        ("https://www.agendaculturel.fr/rss/concert/", "🎵 Concert"),
        ("https://www.agendaculturel.fr/rss/theatre/", "🎭 Théâtre"),
        ("https://www.agendaculturel.fr/rss/exposition/", "🎨 Exposition"),
        ("https://www.agendaculturel.fr/rss/festival/", "🎪 Festival"),
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }
    for url, categorie in flux_rss:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            logger.info(f"AgendaCulturel {categorie} status : {resp.status_code}")
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")
            logger.info(f"AgendaCulturel {categorie} : {len(items)} item(s)")
            for item in items[:5]:
                try:
                    titre = item.findtext("title", "").strip()
                    if not titre or len(titre) < 3:
                        continue
                    url_deal = item.findtext("link", "").strip()
                    if not url_deal:
                        continue
                    description = item.findtext("description", "").strip()
                    # Nettoyer le HTML dans la description
                    if description:
                        desc_soup = BeautifulSoup(description, "html.parser")
                        description = desc_soup.get_text(strip=True)[:150]
                    # Lieu depuis le titre ou description
                    lieu = "France"
                    pub_date = item.findtext("pubDate", "")
                    deal_id = "ac_" + hashlib.md5(url_deal.encode()).hexdigest()[:12]
                    deals.append({
                        "id": deal_id,
                        "titre": titre,
                        "lieu": lieu,
                        "categorie": categorie,
                        "image": None,
                        "url": url_deal,
                        "source": "AgendaCulturel",
                        "description": description
                    })
                except:
                    continue
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Erreur AgendaCulturel {categorie}: {e}")
    return deals

# --- SOURCE 3 : SORTIRAUJOURDHUI (RSS national) ---
def get_sortiraujourdhui():
    deals = []
    flux_rss = [
        ("https://www.sortiraujourdhui.fr/rss/concert", "🎵 Concert"),
        ("https://www.sortiraujourdhui.fr/rss/spectacle", "🎭 Spectacle"),
        ("https://www.sortiraujourdhui.fr/rss/exposition", "🎨 Exposition"),
        ("https://www.sortiraujourdhui.fr/rss/famille", "👨‍👩‍👧 Famille"),
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml",
    }
    for url, categorie in flux_rss:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            logger.info(f"Sortiraujourdhui {categorie} status : {resp.status_code}")
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")
            logger.info(f"Sortiraujourdhui {categorie} : {len(items)} item(s)")
            for item in items[:5]:
                try:
                    titre = item.findtext("title", "").strip()
                    if not titre or len(titre) < 3:
                        continue
                    url_deal = item.findtext("link", "").strip()
                    if not url_deal:
                        continue
                    description = item.findtext("description", "").strip()
                    if description:
                        desc_soup = BeautifulSoup(description, "html.parser")
                        description = desc_soup.get_text(strip=True)[:150]
                    # Extraire image depuis contenu RSS
                    image = None
                    enclosure = item.find("enclosure")
                    if enclosure is not None:
                        image = enclosure.get("url")
                    deal_id = "saj_" + hashlib.md5(url_deal.encode()).hexdigest()[:12]
                    deals.append({
                        "id": deal_id,
                        "titre": titre,
                        "lieu": "France",
                        "categorie": categorie,
                        "image": image,
                        "url": url_deal,
                        "source": "Sortiraujourdhui",
                        "description": description
                    })
                except:
                    continue
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Erreur Sortiraujourdhui {categorie}: {e}")
    return deals

# --- SOURCE 4 : OPENAGENDA ---
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
                    "id": deal_id, "titre": nom, "lieu": lieu,
                    "categorie": "📅 Événement / Expo",
                    "image": e.get("image"),
                    "url": f"https://openagenda.com/event/{e.get('slug', '')}",
                    "source": "OpenAgenda",
                    "description": e.get("description", {}).get("fr", "") if isinstance(e.get("description"), dict) else ""
                })
    except Exception as e:
        logger.error(f"Erreur OpenAgenda: {e}")
    return deals

# --- SOURCE 5 : QUE FAIRE A PARIS ---
def get_paris_events():
    deals = []
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {"dataset": "que-faire-a-paris-", "rows": 8, "sort": "date_start"}
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
                    "id": deal_id, "titre": nom, "lieu": lieu,
                    "categorie": f"🎭 {categorie_raw}{date_str}",
                    "image": f.get("cover_url"),
                    "url": f.get("url", "https://quefaire.paris.fr"),
                    "source": "Que faire à Paris",
                    "description": f.get("lead_text", "")
                })
    except Exception as e:
        logger.error(f"Erreur Paris Events: {e}")
    return deals

# --- SOURCE 6 : DATATOURISME ---
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
                    "id": deal_id, "titre": nom, "lieu": localisation,
                    "categorie": "🎡 Sortie & Loisirs", "image": None,
                    "url": item.get("@id", "https://www.datatourisme.fr"),
                    "source": "DATAtourisme",
                    "description": "Lieu de loisirs référencé par le Ministère du Tourisme français."
                })
    except Exception as e:
        logger.error(f"Erreur DATAtourisme: {e}")
    return deals

# --- SOURCE 7 : BILLETREDUC ---
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
                    reduction_txt = reduction_el.get_text(strip=True) if reduction_
