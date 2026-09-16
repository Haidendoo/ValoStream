#!/usr/bin/env python3
"""
Delivery & Clickstream Real-Time Event Generator (ValoStream)
Simulates:
1. Customer browsing & ordering clickstream with GPS coordinates (events.clickstream)
2. Courier live GPS tracking & telemetry (events.telemetry)
3. Transactional CDC order updates (cdc.orders)
"""

import time
import json
import random
import argparse
import sys

try:
    from kafka import KafkaProducer
except ImportError:
    try:
        from confluent_kafka import Producer as ConfluentProducer
        class KafkaProducer:
            def __init__(self, bootstrap_servers, value_serializer=None):
                self.p = ConfluentProducer({'bootstrap.servers': bootstrap_servers})
                self.ser = value_serializer or (lambda v: json.dumps(v).encode('utf-8'))
            def send(self, topic, value):
                self.p.produce(topic, self.ser(value))
            def flush(self):
                self.p.flush()
    except ImportError:
        KafkaProducer = None

# Ho Chi Minh City center bounding box for realistic geospatial coordinates
LAT_MIN, LAT_MAX = 10.7450, 10.8150
LON_MIN, LON_MAX = 106.6600, 106.7300

# Synthetic Entity Pools
MERCHANTS = [
    {"id": f"MCH_{i:04d}", "name": f"Restaurant {i}", "lat": random.uniform(LAT_MIN, LAT_MAX), "lon": random.uniform(LON_MIN, LON_MAX)}
    for i in range(1, 51)
]
USERS = [f"USR_{i:06d}" for i in range(1, 501)]
COURIERS = [f"DRV_{i:04d}" for i in range(1, 101)]
ITEMS = [f"ITM_{i:05d}" for i in range(1, 301)]
DEVICE_TYPES = ["ios", "android", "web"]
EVENT_TYPES = ["view_item", "click", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.60, 0.25, 0.10, 0.05]
REFERRERS = ["home_feed", "search", "nearby_deals", "recs_tray"]

def generate_clickstream_event():
    user_id = random.choice(USERS)
    merchant = random.choice(MERCHANTS)
    item_id = random.choice(ITEMS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    
    # User location is clustered around merchants or city center
    lat = merchant["lat"] + random.gauss(0, 0.008)
    lon = merchant["lon"] + random.gauss(0, 0.008)
    now_ms = int(time.time() * 1000)

    return {
        "event_id": f"CLK_{now_ms}_{random.randint(1000, 9999)}",
        "user_id": user_id,
        "item_id": item_id,
        "merchant_id": merchant["id"],
        "event_type": event_type,
        "device_type": random.choice(DEVICE_TYPES),
        "dwell_time_ms": random.randint(800, 45000) if event_type != "click" else random.randint(100, 1500),
        "referrer_page": random.choice(REFERRERS),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "event_ts_ms": now_ms
    }

def generate_courier_telemetry_event():
    courier_id = random.choice(COURIERS)
    lat = random.uniform(LAT_MIN, LAT_MAX)
    lon = random.uniform(LON_MIN, LON_MAX)
    now_ms = int(time.time() * 1000)

    return {
        "telemetry_id": f"TEL_{now_ms}_{random.randint(1000, 9999)}",
        "courier_id": courier_id,
        "order_id": f"ORD_{random.randint(10000, 99999)}" if random.random() > 0.3 else None,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "speed_kmh": round(random.uniform(5.0, 45.0), 1),
        "bearing_deg": random.randint(0, 359),
        "status": random.choice(["to_merchant", "at_merchant", "delivering", "idle"]),
        "event_ts_ms": now_ms
    }

def main():
    parser = argparse.ArgumentParser(description="ValoStream Delivery & Clickstream Stream Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--rate", type=int, default=50, help="Target events per second")
    parser.add_argument("--max-events", type=int, default=0, help="Total events to produce (0 = infinite)")
    parser.add_argument("--dry-run", action="store_true", help="Print events to stdout instead of sending to Kafka")
    args = parser.parse_args()

    producer = None
    if not args.dry_run:
        if KafkaProducer is None:
            print("❌ kafka-python or confluent-kafka not found. Run with --dry-run or install dependencies.")
            sys.exit(1)
        print(f"📡 Connecting to Kafka at {args.bootstrap_servers}...")
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Connected to Kafka.")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            sys.exit(1)

    print(f"🚀 Starting delivery stream at {args.rate} events/sec (max: {args.max_events or 'unlimited'})...")
    total_sent = 0
    t0 = time.time()

    try:
        while True:
            # Emit Clickstream Event (70% probability)
            if random.random() < 0.70:
                evt = generate_clickstream_event()
                topic = "events.clickstream"
            # Emit Courier Telemetry (30% probability)
            else:
                evt = generate_courier_telemetry_event()
                topic = "events.telemetry"

            if args.dry_run:
                print(f"[{topic}] {json.dumps(evt)}")
            else:
                producer.send(topic, evt)

            total_sent += 1
            if args.max_events > 0 and total_sent >= args.max_events:
                break

            time.sleep(1.0 / max(1, args.rate))

            if total_sent % 500 == 0:
                elapsed = time.time() - t0
                print(f"⚡ Emitted {total_sent} events ({total_sent / elapsed:.1f} evt/sec)")

    except KeyboardInterrupt:
        print("\n⏹️ Stopped by user.")
    finally:
        if producer:
            producer.flush()
        print(f"🏁 Done. Total events sent: {total_sent}")

if __name__ == "__main__":
    main()
