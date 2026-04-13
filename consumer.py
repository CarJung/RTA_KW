from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='anomaly-detector'
)

# Słownik: user_id -> lista timestampów transakcji
user_transactions = defaultdict(list)

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    timestamp = datetime.fromisoformat(tx['timestamp'])
    
    # Usuwamy transakcje starsze niż 60 sekund
    cutoff_time = timestamp - timedelta(seconds=60)
    user_transactions[user_id] = [
        ts for ts in user_transactions[user_id]
        if ts > cutoff_time
    ]
    
    # Dodajemy nową transakcję
    user_transactions[user_id].append(timestamp)
    
    # Sprawdzamy anomalię
    tx_count = len(user_transactions[user_id])
    if tx_count > 3:
        print(
            f"🚨 ALERT: {user_id} wykonał {tx_count} transakcji"
            f" w ciągu 60s!"
        )
        print(
            f"   {tx['tx_id']} | {tx['amount']:.2f} PLN |"
            f" {tx['store']}"
        )
    else:
        print(
            f"[OK] {user_id}: {tx_count}/3 ({tx['amount']:.2f} PLN"
            f" @ {tx['store']})"
        )
