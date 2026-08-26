from datetime import date, datetime, time, timedelta
import random
import uuid

from pyspark.sql import SparkSession


CATALOG = "workspace"
LANDING_SCHEMA = "bookmyshow_landing"
RAW_VOLUME = "raw_feed"
RAW_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{RAW_VOLUME}"
SEED = 42


def write_json(df, entity_name, partitions=1):
    (
        df.coalesce(partitions)
        .write.mode("overwrite")
        .json(f"{RAW_BASE_PATH}/{entity_name}")
    )


def build_theaters():
    cities = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Hyderabad", "Telangana"),
        ("Chennai", "Tamil Nadu"),
        ("Ahmedabad", "Gujarat"),
        ("Kolkata", "West Bengal"),
        ("Jaipur", "Rajasthan"),
        ("Kochi", "Kerala"),
    ]
    brands = ["PVR", "INOX", "Cinepolis", "Miraj", "Carnival", "Movietime"]
    records = []
    for idx in range(1, 26):
        city, state = random.choice(cities)
        screens = random.randint(3, 12)
        seat_capacity = screens * random.randint(120, 220)
        records.append(
            {
                "theater_id": f"TH{idx:04d}",
                "theater_name": f"{random.choice(brands)} {city} {random.choice(['Mall', 'Center', 'Plaza', 'Square'])}",
                "city": city,
                "state": state,
                "screens_count": screens,
                "seat_capacity": seat_capacity,
                "opened_on": str(date.today() - timedelta(days=random.randint(900, 5000))),
                "is_active": random.random() > 0.05,
                "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 72))).isoformat(),
            }
        )
    return records


def build_movies():
    titles = [
        "Skyline Chase",
        "Monsoon Hearts",
        "Code Red",
        "Empire of Ashes",
        "Silent Verdict",
        "Cosmic Run",
        "Neon Dreams",
        "The Last Interval",
        "Hidden Score",
        "Desert Signal",
        "The Final Cue",
        "Parallel Tracks",
    ]
    genres = ["Action", "Drama", "Comedy", "Thriller", "Sci-Fi", "Romance", "Family"]
    languages = ["Hindi", "English", "Tamil", "Telugu", "Kannada"]
    certifications = ["U", "UA", "A"]
    formats = ["2D", "3D", "IMAX"]
    distributors = ["Star Studios", "Red Giant", "Prime Motion", "Vista Films", "Galaxy Pictures"]
    records = []
    for idx in range(1, 61):
        title = f"{random.choice(titles)} {idx}"
        records.append(
            {
                "movie_id": f"MV{idx:04d}",
                "title": title,
                "genre": random.choice(genres),
                "language": random.choice(languages),
                "duration_mins": random.randint(95, 185),
                "certification": random.choice(certifications),
                "release_date": str(date.today() - timedelta(days=random.randint(1, 500))),
                "imdb_rating": round(random.uniform(5.2, 9.3), 1),
                "movie_format": random.choice(formats),
                "distributor": random.choice(distributors),
                "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 72))).isoformat(),
            }
        )
    return records


def build_customers():
    first_names = ["Aarav", "Diya", "Rahul", "Ananya", "Kabir", "Meera", "Ishaan", "Sara", "Dev", "Riya"]
    last_names = ["Sharma", "Patel", "Reddy", "Nair", "Verma", "Mehta", "Rao", "Kapoor", "Iyer", "Singh"]
    cities = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Hyderabad", "Telangana"),
        ("Chennai", "Tamil Nadu"),
        ("Ahmedabad", "Gujarat"),
        ("Kolkata", "West Bengal"),
    ]
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    records = []
    for idx in range(1, 2001):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city, state = random.choice(cities)
        records.append(
            {
                "customer_id": f"CU{idx:06d}",
                "full_name": f"{first_name} {last_name}",
                "email": f"{first_name.lower()}.{last_name.lower()}{idx}@example.com",
                "phone": f"9{random.randint(100000000, 999999999)}",
                "city": city,
                "state": state,
                "loyalty_tier": random.choices(tiers, weights=[0.55, 0.25, 0.15, 0.05])[0],
                "marketing_opt_in": random.random() > 0.35,
                "signup_ts": (datetime.utcnow() - timedelta(days=random.randint(30, 1500))).isoformat(),
                "birth_year": random.randint(1972, 2006),
                "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 72))).isoformat(),
            }
        )
    return records


def build_shows(theaters, movies):
    screen_formats = ["2D", "3D", "IMAX"]
    statuses = ["SCHEDULED", "COMPLETED", "CANCELLED"]
    start_date = date.today() - timedelta(days=30)
    records = []
    show_counter = 1
    for show_date_offset in range(0, 45):
        current_date = start_date + timedelta(days=show_date_offset)
        for theater in theaters:
            active_screens = min(theater["screens_count"], 4)
            for screen_id in range(1, active_screens + 1):
                for slot_hour in [10, 13, 16, 19, 22]:
                    movie = random.choice(movies)
                    start_ts = datetime.combine(current_date, time(slot_hour, 0))
                    end_ts = start_ts + timedelta(minutes=movie["duration_mins"] + 25)
                    status = random.choices(statuses, weights=[0.72, 0.24, 0.04])[0]
                    records.append(
                        {
                            "show_id": f"SH{show_counter:07d}",
                            "theater_id": theater["theater_id"],
                            "screen_id": f"SCR{screen_id:02d}",
                            "movie_id": movie["movie_id"],
                            "show_date": str(current_date),
                            "show_start_ts": start_ts.isoformat(),
                            "show_end_ts": end_ts.isoformat(),
                            "show_format": random.choice(screen_formats),
                            "show_language": movie["language"],
                            "base_price": round(random.uniform(140, 520), 2),
                            "available_seats": max(60, int(theater["seat_capacity"] / theater["screens_count"])),
                            "show_status": status,
                            "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 72))).isoformat(),
                        }
                    )
                    show_counter += 1
    return records


def build_bookings(customers, shows, movies_by_id):
    statuses = ["CONFIRMED", "CANCELLED", "PENDING"]
    channels = ["app", "mobile_web", "desktop_web", "partner_wallet"]
    coupons = [None, "WEEKEND10", "MOVIENIGHT", "FIRSTBOOK", "CARD20"]
    records = []
    candidate_shows = [show for show in shows if show["show_status"] != "CANCELLED"]
    for idx in range(1, 25001):
        show = random.choice(candidate_shows)
        movie = movies_by_id[show["movie_id"]]
        customer = random.choice(customers)
        tickets = random.randint(1, 6)
        gross_amount = round(show["base_price"] * tickets, 2)
        discount_amount = round(gross_amount * random.choice([0.0, 0.05, 0.1, 0.15]), 2)
        convenience_fee = round(tickets * random.uniform(22, 38), 2)
        net_amount = round(gross_amount - discount_amount + convenience_fee, 2)
        booking_status = random.choices(statuses, weights=[0.9, 0.06, 0.04])[0]
        show_start_ts = datetime.fromisoformat(show["show_start_ts"])
        booked_at = show_start_ts - timedelta(days=random.randint(0, 10), hours=random.randint(1, 20), minutes=random.randint(5, 59))
        records.append(
            {
                "booking_id": f"BK{idx:08d}",
                "customer_id": customer["customer_id"],
                "show_id": show["show_id"],
                "theater_id": show["theater_id"],
                "movie_id": show["movie_id"],
                "booked_at": booked_at.isoformat(),
                "tickets_count": tickets,
                "gross_amount": gross_amount,
                "discount_amount": discount_amount,
                "convenience_fee": convenience_fee,
                "net_amount": net_amount,
                "booking_status": booking_status,
                "booking_channel": random.choice(channels),
                "coupon_code": random.choice(coupons),
                "customer_city": customer["city"],
                "movie_genre": movie["genre"],
                "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 48))).isoformat(),
            }
        )
    return records


def build_payments(bookings):
    methods = ["UPI", "Card", "NetBanking", "Wallet"]
    gateways = ["Razorpay", "Paytm", "Stripe", "Cashfree"]
    statuses = ["SUCCESS", "FAILED", "REFUNDED", "PENDING"]
    records = []
    for idx, booking in enumerate(bookings, start=1):
        if booking["booking_status"] == "CONFIRMED":
            payment_status = random.choices(statuses, weights=[0.88, 0.04, 0.04, 0.04])[0]
        elif booking["booking_status"] == "CANCELLED":
            payment_status = random.choices(statuses, weights=[0.18, 0.08, 0.68, 0.06])[0]
        else:
            payment_status = random.choices(statuses, weights=[0.25, 0.2, 0.05, 0.5])[0]
        transaction_amount = booking["net_amount"]
        refund_amount = transaction_amount if payment_status == "REFUNDED" else 0.0
        records.append(
            {
                "payment_id": f"PY{idx:08d}",
                "booking_id": booking["booking_id"],
                "payment_ts": (datetime.fromisoformat(booking["booked_at"]) + timedelta(minutes=random.randint(1, 20))).isoformat(),
                "payment_method": random.choice(methods),
                "gateway_name": random.choice(gateways),
                "payment_status": payment_status,
                "transaction_amount": transaction_amount,
                "refund_amount": refund_amount,
                "gateway_latency_ms": random.randint(120, 3200),
                "retry_count": random.choices([0, 1, 2, 3], weights=[0.72, 0.18, 0.08, 0.02])[0],
                "payment_reference": str(uuid.uuid4()),
                "updated_at": (datetime.utcnow() - timedelta(hours=random.randint(1, 48))).isoformat(),
            }
        )
    return records


def main():
    random.seed(SEED)
    spark = SparkSession.builder.getOrCreate()

    theaters = build_theaters()
    movies = build_movies()
    customers = build_customers()
    shows = build_shows(theaters, movies)
    movies_by_id = {movie["movie_id"]: movie for movie in movies}
    bookings = build_bookings(customers, shows, movies_by_id)
    payments = build_payments(bookings)

    write_json(spark.createDataFrame(theaters), "theaters")
    write_json(spark.createDataFrame(movies), "movies")
    write_json(spark.createDataFrame(customers), "customers", partitions=2)
    write_json(spark.createDataFrame(shows), "shows", partitions=4)
    write_json(spark.createDataFrame(bookings), "bookings", partitions=4)
    write_json(spark.createDataFrame(payments), "payments", partitions=4)

    print("Synthetic BookMyShow-style source data created under", RAW_BASE_PATH)


if __name__ == "__main__":
    main()
