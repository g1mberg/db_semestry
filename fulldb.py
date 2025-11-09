import asyncio
import random
from faker import Faker
import string
import requests
import aiohttp
import psycopg2
from datetime import datetime, timedelta, timezone
import time
from collections import deque
from faker.generator import random

conn = psycopg2.connect(
    host="localhost",
    database="dota",
    user="user_owner",
    password="123456"
)
cur = conn.cursor()
fake = Faker()

def insert_data(table, columns, data):
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
    for row in data:
        cur.execute(sql, row)
    conn.commit()

def fetch_heroes():
    url = "https://api.opendota.com/api/heroes"
    response = requests.get(url)
    heroes = response.json()

    hero_data = []
    for hero in heroes:
        hero_data.append((
            hero["id"],
            hero["localized_name"],
            hero["primary_attr"],
            hero["attack_type"] == "Ranged"
        ))
    insert_data("static.\"heroes\"", ["hero_id", "hero_name", "attribute", "attack_type"], hero_data)
    print(f"Inserted {len(hero_data)} heroes.")

def fetch_items():
    url = "https://api.opendota.com/api/constants/items"
    response = requests.get(url)
    items = response.json()

    item_data = []
    for key, item in items.items():
        name = item.get("dname")
        cost = item.get("cost")
        recipe = item.get("components", None)
        if name is None or cost is None or cost == 0: continue
        print(name, cost, recipe)
        item_data.append((name, cost, recipe))

    insert_data("static.\"items\"", ["item_name", "cost", "recipe"], item_data)
    print(f"Inserted {len(item_data)} items.")

def fetch_neutral_items():
    url = "https://api.opendota.com/api/constants/items"
    response = requests.get(url)
    neutral_items = response.json()

    neutral_data = []
    for key, item in neutral_items.items():
        name = item.get("dname", key)
        tier = item.get("tier", None)
        if tier is None: continue
        neutral_data.append((name, tier))

    insert_data("static.\"neutral_items\"", ["neutral_items_name", "tier"], neutral_data)
    print(f"Inserted {len(neutral_data)} neutral items.")

def fetch_neutral_enchants():
    url = "https://api.opendota.com/api/constants/items"
    response = requests.get(url)
    neutral_items = response.json()

    neutral_data = []
    for key, item in neutral_items.items():
        if 'enhancement' not in key: continue
        name = item.get("dname", key)
        neutral_data.append((name,))

    insert_data("static.\"neutral_enchant\"", ["neutral_enchant_name"], neutral_data)
    print(f"Inserted {len(neutral_data)} neutral enchants.")

def generate_users(n_rows=2000):
    steam_ids = set()
    logins = set()

    while len(steam_ids) < n_rows:
        steam_ids.add(random.randint(1_000_000_000_000, 9_999_999_999_999))
    while len(logins) < n_rows:
        logins.add(fake.user_name())

    steam_ids = list(steam_ids)
    logins = list(logins)

    users_data = []
    for i in range(n_rows):
        full_name = fake.name() if random.random() > 0.15 else None
        nickname = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
        country = fake.country() if random.random() > 0.15 else None
        birthday = fake.date_of_birth(minimum_age=16, maximum_age=45) if random.random() > 0.15 else None

        users_data.append((steam_ids[i], logins[i], full_name, nickname, country, birthday))

    insert_data("player_info.steam_account",
                ["steam_id", "login", "full_name", "nickname", "country", "birthday"],
                users_data)
    print(f"Inserted {len(users_data)} steam accounts.")

def fetch_steam_ids(n_rows):
    cur.execute(f"""
        SELECT steam_id FROM player_info.steam_account
        ORDER BY RANDOM()
        LIMIT {n_rows}
    """)
    result = cur.fetchall()

    return [row[0] for row in result]

def generate_players(steam_ids, n_rows=1252):
    playerid = set()
    while len(playerid) < n_rows:
        playerid.add(random.randint(1_000_000_000, 999_999_999_999))
    playerid = list(playerid)

    ranks_used = set()
    players_data = []

    for i in range(n_rows):
        rank = random.randint(0, 1000)
        if rank in ranks_used or random.random() < 0.1:
            rank_value = None
        else:
            rank_value = rank
            ranks_used.add(rank)

        prof_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        nickname = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

        players_data.append((playerid[i], steam_ids[i], prof_name, rank_value, nickname))

    insert_data("player_info.players",
                ["player_id", "steam_id", "prof_name", "rank", "nickname"],
                players_data)

def generate_matches(n_matches=500):
    match_ids = set()
    while len(match_ids) < n_matches:
        match_ids.add(random.randint(1_000_000, 9_999_999))
    match_ids = list(match_ids)

    matches_data = []
    for i in range(n_matches):
        duration = random.randint(600, 5400)
        winner = random.choice([True, False])

        base_date = datetime.utcnow() - timedelta(days=730)
        date = base_date + timedelta(
            days=random.randint(0, 730),
            seconds=random.randint(0, 86399),
            microseconds=random.randint(0, 999000)
        )

        tz_offset = random.randint(-12, 14)
        tz = timezone(timedelta(hours=tz_offset))
        date = date.replace(tzinfo=timezone.utc).astimezone(tz)
        date_str = date.strftime('%Y-%m-%d %H:%M:%S.%f %z')

        matches_data.append((match_ids[i], duration, winner, date_str))

    insert_data(
        "match_info.matches",
        ["match_id", "duration", "winner", "date"],
        matches_data
    )

    print(f"{n_matches} матчей успешно вставлено в match_info.matches.")

def generate_player_match_stat():
    cur.execute("SELECT match_id FROM match_info.matches")
    match_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT player_id FROM player_info.players")
    player_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT hero_id FROM static.heroes")
    hero_ids = [row[0] for row in cur.fetchall()]

    all_stats = []

    for match_id in match_ids:
        players = random.sample(player_ids, 10)
        for a, player_id in enumerate(players):
            stat = (
                match_id,
                player_id,
                random.choice(hero_ids),
                a < 5,
                a % 5 + 1,
                random.randint(0, 25),
                random.randint(0, 25),
                random.randint(4, 30),
                random.randint(200, 700),
                random.randint(200, 600),
                random.randint(20, 500),
                random.randint(10, 100)
            )
            all_stats.append(stat)

    insert_data(
        "match_info.player_match_stat",
        ["match_id", "player_id", "hero_id", "side", "pos", "kills", "deaths",
         "assists", "gpm", "xpm", "last_hit", "denies"],
        all_stats
    )

    print(f"{len(all_stats)} записей статистики игроков успешно вставлено.")

def generate_player_neutral_items():
    cur.execute("""
        SELECT stat.stat_id, m.duration
        FROM match_info.player_match_stat AS stat
        JOIN match_info.matches AS m ON stat.match_id = m.match_id
    """)
    stat_rows = cur.fetchall()

    cur.execute("SELECT neutral_items_id FROM static.neutral_items")
    neutral_item_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT neutral_enchant_id FROM static.neutral_enchant")
    neutral_enchant_ids = [row[0] for row in cur.fetchall()]

    all_player_neutrals = []

    for stat_id, duration in stat_rows:
        neutral_sample = random.sample(neutral_item_ids, 5)
        tiers = sorted([random.randint(1, 5) for _ in range(5)])
        for neutral_id, tier in zip(neutral_sample, tiers):
            enchant = random.choice(neutral_enchant_ids) if neutral_enchant_ids else None
            time_from_start = random.randint(60, max(60, duration // 5 * tier))
            all_player_neutrals.append((stat_id, neutral_id, enchant, time_from_start, tier))

    insert_data(
        "match_info.player_neutral_items",
        ["stat_id", "neutral_item_id", "neutral_enchant_id", "time_from_start", "tier"],
        all_player_neutrals
    )

    print(f"{len(all_player_neutrals)} нейтральных предметов успешно вставлено.")

def generate_player_items():
    cur.execute("""
        SELECT stat.stat_id, m.duration
        FROM match_info.player_match_stat AS stat
        JOIN match_info.matches AS m ON stat.match_id = m.match_id
    """)
    stat_rows = cur.fetchall()

    cur.execute("SELECT item_id FROM static.items")
    item_ids = [row[0] for row in cur.fetchall()]

    all_player_items = []

    for stat_id, duration in stat_rows:
        n_items = random.randint(0, 6)
        if n_items > 0:
            items = random.sample(item_ids, n_items)
            for item in items:
                time_from_start = random.randint(60, max(60, duration))
                all_player_items.append((stat_id, item, time_from_start))

    insert_data(
        "match_info.player_items",
        ["stat_id", "item_id", "time_from_start"],
        all_player_items
    )

    print(f"{len(all_player_items)} обычных предметов успешно вставлено.")


if __name__ == "__main__":
    fetch_heroes()
    fetch_items()
    fetch_neutral_items()
    fetch_neutral_enchants()
    generate_users()
    generate_players(fetch_steam_ids(1252), n_rows=1252)
    generate_matches()
    generate_player_match_stat()
    generate_player_neutral_items()
    generate_player_items()
    cur.close()
    conn.close()
