"""Generate synthetic, deliberately messy e-commerce order CSVs (two arrival batches)."""
import csv, random, sys
from datetime import date, timedelta
from pathlib import Path

CATALOG = {  # product -> (category, base price)
    "Wireless Mouse": ("Electronics", 699), "USB-C Cable": ("Electronics", 299),
    "Bluetooth Speaker": ("Electronics", 1899), "Cotton T-Shirt": ("Fashion", 499),
    "Running Shoes": ("Fashion", 2199), "Denim Jacket": ("Fashion", 2799),
    "Steel Water Bottle": ("Home", 449), "Desk Lamp": ("Home", 899),
    "Notebook Pack": ("Stationery", 249), "Gel Pen Set": ("Stationery", 149),
    "Almonds 500g": ("Grocery", 429), "Green Tea Box": ("Grocery", 259),
}
CITIES = ["Pune", "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Aurangabad", "Chennai"]
PAY = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Net Banking"]
STATUS = ["Delivered", "Delivered", "Delivered", "Shipped", "Cancelled", "Returned"]
HEADER = ["order_id", "customer_id", "order_date", "city", "product", "category",
          "quantity", "unit_price", "discount_pct", "payment_mode", "order_status"]

def make_batch(path, start_id, n, seed, start_day):
    rnd = random.Random(seed)
    rows = []
    for i in range(n):
        prod = rnd.choice(list(CATALOG)); cat, price = CATALOG[prod]
        d = start_day + timedelta(days=rnd.randint(0, 14))
        rows.append([f"ORD{start_id + i:06d}", f"CUST{rnd.randint(1, 400):04d}", d.isoformat(),
                     rnd.choice(CITIES), prod, cat, rnd.randint(1, 5),
                     round(price * rnd.uniform(0.95, 1.05), 2), rnd.choice([0, 0, 5, 10, 15, 20]),
                     rnd.choice(PAY), rnd.choice(STATUS)])
    # inject known data-quality problems (messy casing, blanks, bad values, duplicates)
    for r in rnd.sample(rows, n // 25): r[3] = "  " + r[3].upper() + " "          # messy text
    for r in rnd.sample(rows, n // 30): r[3] = ""                                  # missing city
    for r in rnd.sample(rows, n // 30): r[6] = rnd.choice([0, -2])                 # bad quantity
    for r in rnd.sample(rows, n // 30): r[7] = rnd.choice([0, -50])                # bad price
    for r in rnd.sample(rows, n // 40): r[8] = 150                                 # bad discount
    for r in rnd.sample(rows, n // 40): r[2] = "31/02/2026"                        # impossible date
    for r in rnd.sample(rows, n // 40): r[9] = "Barter"                            # unknown payment
    for r in rnd.sample(rows, n // 50): r[6] = "two"                               # non-numeric
    rows += [list(r) for r in rnd.sample(rows, n // 30)]                           # duplicate orders
    rnd.shuffle(rows)
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(HEADER); w.writerows(rows)
    return len(rows)

if __name__ == "__main__":
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "data/landing"); out.mkdir(parents=True, exist_ok=True)
    print(make_batch(out / "orders_batch_1.csv", 1, 3000, 7, date(2026, 1, 1)), "rows -> batch_1")
    print(make_batch(out / "orders_batch_2.csv", 3001, 2000, 11, date(2026, 1, 10)), "rows -> batch_2")
