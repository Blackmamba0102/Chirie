import time
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import telebot
import threading

# === CONFIG ===
BOT_TOKEN = "7841140498:AAEgOzcYSUj6L974d043_g_eV2n8pGon1sA"   # <-- Token de la BotFather
MY_CHAT_ID = 1104625656                                         # <-- Chat ID (ex. de la @userinfobot)
bot = telebot.TeleBot(BOT_TOKEN)

BASE_URL = "https://www.kleinanzeigen.de/s-vermietungen/c203"
INTERVAL = 180          # secunde între verificări
MAX_RENT = 900          # limita de chirie (euro)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

# === Zone ~30 km în jur de Rüsselsheim am Main ===
ZONE_APPROX_30KM = [
    "Rüsselsheim", "Mainz", "Wiesbaden", "Frankfurt am Main", "Darmstadt",
    "Offenbach am Main", "Groß-Gerau", "Raunheim", "Kelsterbach",
    "Flörsheim am Main", "Hochheim am Main", "Trebur", "Nauheim",
    "Bischofsheim", "Mörfelden-Walldorf", "Griesheim", "Ginsheim-Gustavsburg",
    "Riedstadt", "Kriftel", "Hattersheim am Main", "Eschborn",
    "Hofheim am Taunus", "Langen", "Egelsbach", "Weiterstadt",
    "Eppstein", "Dietzenbach", "Neu-Isenburg", "Erzhausen",
    "Stockstadt am Rhein", "Biebesheim am Rhein", "Büttelborn",
    "Pfungstadt", "Heusenstamm", "Seeheim-Jugenheim", "Kelkheim",
    "Oberursel", "Bad Soden", "Sulzbach am Taunus", "Wehrheim"
]

DB_FILE = "kleinanzeigen_rent.db"
session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.kleinanzeigen.de/",
    "Connection": "keep-alive"
})

# === THREAD-SAFE DB ===
db_lock = threading.Lock()

def with_db_lock(func):
    def wrapper(*args, **kwargs):
        with db_lock:
            return func(*args, **kwargs)
    return wrapper


# === Baza de date ===
@with_db_lock
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS seen (
                 ad_id TEXT PRIMARY KEY,
                 title TEXT,
                 url TEXT,
                 price TEXT,
                 location TEXT,
                 seen_at INTEGER
                 )""")
    c.execute("""CREATE TABLE IF NOT EXISTS users (
                 chat_id INTEGER PRIMARY KEY
                 )""")
    conn.commit()
    conn.close()

@with_db_lock
def add_user(chat_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users(chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    conn.close()

@with_db_lock
def get_users():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT chat_id FROM users")
    users = [row[0] for row in c.fetchall()]
    conn.close()
    return users

@with_db_lock
def is_seen(ad_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen WHERE ad_id=?", (ad_id,))
    result = c.fetchone() is not None
    conn.close()
    return result

@with_db_lock
def mark_seen(ad):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""INSERT OR IGNORE INTO seen(ad_id, title, url, price, location, seen_at)
                 VALUES (?, ?, ?, ?, ?, ?)""",
              (ad["id"], ad["title"], ad["url"], ad["price"], ad["location"], int(time.time())))
    conn.commit()
    conn.close()


# === SCRAPING ===
def fetch_listings():
    try:
        r = session.get(BASE_URL, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print("[Eroare FETCH]:", e)
        return None

def parse_ads(html):
    soup = BeautifulSoup(html, "lxml")
    ads = []

    for item in soup.select("article"):
        ad_id = item.get("data-adid")
        if not ad_id:
            continue

        title_tag = item.select_one("h2, h2 a, .aditem-main--middle--title a")
        title = title_tag.get_text(strip=True) if title_tag else "(fără titlu)"

        link_tag = None
        for a in item.find_all("a", href=True):
            if "/s-anzeige/" in a["href"]:
                link_tag = a
                break
        url = f"https://www.kleinanzeigen.de{link_tag['href']}" if link_tag else BASE_URL

        price_text = "N/V"
        price_value = None
        for p in item.find_all(["p", "span"]):
            text = p.get_text(strip=True)
            if "€" in text or "EUR" in text:
                price_text = text
                m = re.search(r'(\d+)', text.replace('.', '').replace(',', ''))
                if m:
                    price_value = int(m.group(1))
                break

        location_tag = item.select_one(".aditem-main--top--left")
        location = location_tag.get_text(strip=True) if location_tag else ""

        ads.append({
            "id": ad_id,
            "title": title,
            "url": url,
            "price": price_text,
            "price_value": price_value,
            "location": location
        })
    return ads


# === Trimite mesaje ===
def send_to_all(message):
    users = get_users()
    for chat_id in users:
        try:
            bot.send_message(chat_id, message)
            time.sleep(0.5)
        except Exception as e:
            print("[Eroare trimitere Telegram]:", e)


# === Comenzi Telegram ===
@bot.message_handler(commands=['start'])
def start(message):
    add_user(message.chat.id)
    bot.send_message(message.chat.id, "👋 Salut! Vei primi anunțuri noi cu chirii (până la 900 €) în jurul Rüsselsheim (~30 km).")

@bot.message_handler(commands=['latest'])
def latest(message):
    html = fetch_listings()
    if not html:
        bot.send_message(message.chat.id, "⚠️ Eroare la descărcarea paginii.")
        return

    ads = parse_ads(html)
    msg_count = 0
    for ad in ads[:10]:
        if any(zone.lower() in ad["location"].lower() for zone in ZONE_APPROX_30KM):
            if ad["price_value"] is not None and ad["price_value"] <= MAX_RENT:
                bot.send_message(
                    message.chat.id,
                    f"🏠 {ad['title']}\n💶 {ad['price']}\n📍 {ad['location']}\n🔗 {ad['url']}"
                )
                msg_count += 1
    if msg_count == 0:
        bot.send_message(message.chat.id, "❌ Nu am găsit momentan chirii sub 900 € în zona ta.")


# === LOOP SCRAPING ===
def scrape_loop():
    while True:
        html = fetch_listings()
        if not html:
            time.sleep(INTERVAL)
            continue

        ads = parse_ads(html)
        new_count = 0
        for ad in ads:
            if any(zone.lower() in ad["location"].lower() for zone in ZONE_APPROX_30KM):
                if ad["price_value"] is not None and ad["price_value"] <= MAX_RENT:
                    if not is_seen(ad["id"]):
                        msg = f"🏠 {ad['title']}\n💶 {ad['price']}\n📍 {ad['location']}\n🔗 {ad['url']}"
                        send_to_all(msg)
                        mark_seen(ad)
                        new_count += 1
        print(f"[{time.strftime('%H:%M:%S')}] Verificat: {len(ads)} anunțuri — noi trimise: {new_count}")
        time.sleep(INTERVAL)


# === MAIN ===
if __name__ == "__main__":
    init_db()
    print("✅ Botul de chirii pornit.")

    try:
        bot.send_message(MY_CHAT_ID, "🏠 Botul de chirii a pornit! Voi trimite automat anunțurile sub 900 € din zona Rüsselsheim (~30 km).")
    except Exception as e:
        print("[Avertisment] Nu s-a putut trimite mesajul de start:", e)

    # thread separat pentru scraping
    t = threading.Thread(target=scrape_loop, daemon=True)
    t.start()

    # Polling cu reconectare automată
    while True:
        try:
            bot.polling(none_stop=True, interval=5, timeout=60)
        except Exception as e:
            print("[Eroare la polling] Se reîncearcă în 10 secunde:", e)
            time.sleep(10)
