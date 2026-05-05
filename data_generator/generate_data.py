# ============================================
# RETAIL SYNTHETIC DATA GENERATOR
# Author: Dhrumil Patel
# ============================================

import psycopg2
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── DB connection ────────────────────────────
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="retail_oltp",
    user="retail_user",
    password="retail_pass"
)
cur = conn.cursor()

# ── Config ───────────────────────────────────
NUM_STORES      = 20
NUM_PRODUCTS    = 50
START_DATE      = date(2023, 1, 1)
END_DATE        = date(2024, 12, 31)
SALES_PER_DAY   = 80   # avg transactions per day across all stores

# ── Lookup data ──────────────────────────────
COUNTRIES = [
    ("US", "United States"), ("CA", "Canada"),
    ("GB", "United Kingdom"), ("AU", "Australia")
]
REGIONS = ["North", "South", "East", "West", "Central"]
STORE_TYPES = [("SM", "Supermarket"), ("HY", "Hypermarket"), ("CV", "Convenience")]
CATEGORIES = [
    (1, "Beverages",    [(1, "Soft Drinks"), (2, "Juices"),      (3, "Water")]),
    (2, "Snacks",       [(4, "Chips"),       (5, "Cookies"),     (6, "Nuts")]),
    (3, "Dairy",        [(7, "Milk"),        (8, "Cheese"),      (9, "Yogurt")]),
    (4, "Bakery",       [(10, "Bread"),      (11, "Cakes"),      (12, "Pastries")]),
    (5, "Frozen Foods", [(13, "Meals"),      (14, "Ice Cream"),  (15, "Vegetables")]),
]
BRANDS = ["FreshFarm", "QuickBite", "NaturePlus", "DailyGood",
          "PrimePick", "ValueMart", "OrganicLife", "TasteKing"]
MARKETS = [
    (1, "Northeast", [(1, "New England"), (2, "Mid-Atlantic")]),
    (2, "Southeast", [(3, "Deep South"),  (4, "Florida")]),
    (3, "Midwest",   [(5, "Great Lakes"), (6, "Plains")]),
    (4, "West",      [(7, "Pacific"),     (8, "Mountain")]),
]


# ════════════════════════════════════════════
# 1. STORES
# ════════════════════════════════════════════
print("Inserting stores...")
stores = []
for i in range(1, NUM_STORES + 1):
    cntry_cd, cntry_nm   = random.choice(COUNTRIES)
    store_type_cd, store_type_desc = random.choice(STORE_TYPES)
    market_key, market_name, submarkets = random.choice(MARKETS)
    submarket_key, submarket_name       = random.choice(submarkets)
    row = (
        i, 1000 + i,
        f"{fake.last_name()} {store_type_desc}",
        fake.street_address(), fake.city(),
        random.choice(REGIONS),
        cntry_cd, cntry_nm,
        fake.postcode(),
        fake.state(), fake.state_abbr(),
        store_type_cd, store_type_desc,
        random.choice(["Y", "N"]),
        random.choice(["SMALL", "MEDIUM", "LARGE"]),
        market_key, market_name,
        submarket_key, submarket_name,
        round(random.uniform(25.0, 65.0), 6),
        round(random.uniform(-125.0, -65.0), 6),
    )
    stores.append(row)

cur.executemany("""
    INSERT INTO store VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""", stores)
conn.commit()
print(f"  ✓ {len(stores)} stores inserted")


# ════════════════════════════════════════════
# 2. PRODUCTS
# ════════════════════════════════════════════
print("Inserting products...")
products = []
for i in range(1, NUM_PRODUCTS + 1):
    cat_key, cat_name, subcats = random.choice(CATEGORIES)
    subcat_key, subcat_name    = random.choice(subcats)
    row = (
        i,
        f"{random.choice(BRANDS)} {subcat_name} {fake.word().capitalize()}",
        round(random.uniform(0.1, 5.0), 2),
        round(random.uniform(0.05, 3.0), 2),
        random.choice(BRANDS),
        1, "Active",
        cat_key, cat_name,
        subcat_key, subcat_name,
    )
    products.append(row)

cur.executemany("""
    INSERT INTO product VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""", products)
conn.commit()
print(f"  ✓ {len(products)} products inserted")


# ════════════════════════════════════════════
# 3. CALENDAR
# ════════════════════════════════════════════
print("Inserting calendar...")
calendar_rows = []
d = START_DATE
while d <= END_DATE:
    iso   = d.isocalendar()
    yr    = d.year
    wk    = iso[1]
    mnth  = d.month
    qtr   = (mnth - 1) // 3 + 1
    row = (
        d,
        "Business Day" if d.weekday() < 5 else "Weekend",
        d.weekday() + 1,
        d.strftime("%A"),
        yr,
        wk,
        int(f"{yr}{wk:02d}"),
        mnth,
        int(f"{yr}{mnth:02d}"),
        qtr,
        int(f"{yr}{qtr}"),
    )
    calendar_rows.append(row)
    d += timedelta(days=1)

cur.executemany("""
    INSERT INTO calendar VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING
""", calendar_rows)
conn.commit()
print(f"  ✓ {len(calendar_rows)} calendar days inserted")


# ════════════════════════════════════════════
# 4. SALES  (realistic seasonal patterns)
# ════════════════════════════════════════════
print("Inserting sales  (this may take a moment)...")

store_keys   = [s[0] for s in stores]
product_keys = [p[0] for p in products]
all_dates    = [START_DATE + timedelta(days=i)
                for i in range((END_DATE - START_DATE).days + 1)]

sales_rows = []
trans_id   = 1

for d in all_dates:
    # seasonal multiplier — higher in Nov/Dec
    month_mult = 1.5 if d.month in (11, 12) else \
                 1.2 if d.month in (6, 7, 8) else 1.0
    # weekend bump
    day_mult   = 1.3 if d.weekday() >= 5 else 1.0
    n_trans    = int(SALES_PER_DAY * month_mult * day_mult * random.uniform(0.8, 1.2))

    for _ in range(n_trans):
        prod_key  = random.choice(product_keys)
        store_key = random.choice(store_keys)
        qty       = round(random.uniform(1, 20), 2)
        price     = round(random.uniform(1.5, 50.0), 2)
        discount  = round(price * random.uniform(0, 0.20), 2)
        cost      = round(price * random.uniform(0.4, 0.7), 2)
        sales_rows.append((
            trans_id, prod_key, store_key, d,
            random.randint(800, 2200),
            qty, price,
            round(qty * (price - discount), 2),
            discount, round(qty * cost, 2),
        ))
        trans_id += 1

    # batch insert every 10 000 rows to keep memory low
    if len(sales_rows) >= 10_000:
        cur.executemany("""
            INSERT INTO sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, sales_rows)
        conn.commit()
        sales_rows = []

if sales_rows:
    cur.executemany("""
        INSERT INTO sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, sales_rows)
    conn.commit()

print(f"  ✓ ~{trans_id - 1:,} sales transactions inserted")


# ════════════════════════════════════════════
# 5. INVENTORY  (daily snapshot per store+product)
# ════════════════════════════════════════════
print("Inserting inventory  (this may take a moment)...")

inv_rows = []
for d in all_dates:
    for store_key in store_keys:
        # only track ~30 % of products per store per day to keep volume sane
        sampled_products = random.sample(product_keys, k=max(1, NUM_PRODUCTS // 3))
        for prod_key in sampled_products:
            on_hand  = round(random.uniform(0, 500), 2)
            on_order = round(random.uniform(0, 200), 2)
            oos      = on_hand < 10
            low_stk  = on_hand < 50 and not oos
            inv_rows.append((
                d, store_key, prod_key,
                on_hand, on_order,
                oos,
                round(random.uniform(0, 5), 2),
                random.random() < 0.15,
                low_stk,
            ))

    if len(inv_rows) >= 10_000:
        cur.executemany("""
            INSERT INTO inventory VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, inv_rows)
        conn.commit()
        inv_rows = []

if inv_rows:
    cur.executemany("""
        INSERT INTO inventory VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, inv_rows)
    conn.commit()

print(f"  ✓ inventory snapshots inserted")

cur.close()
conn.close()
print("\n✅ All data generated successfully!")