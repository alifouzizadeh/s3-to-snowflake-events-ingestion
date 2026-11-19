

from dataclasses import asdict
import datetime
from fileinput import filename
import json
import os
import random
import uuid
import boto3

from model import Event, Payload


OUT_DIR = os.environ.get("OUT_DIR", "out")
LAST_DAYS = 3
FILES_PER_DAY = 50
EVENT_TYPES = ["cart_updated", "order_placed", "payment_failed", "order_shipped"]
SKUS = ["iphone-15", "macbook-air", "ps5"]

def make_event(day: datetime.datetime) -> Event:
    ts = (
        datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(seconds=random.randint(0, 86399))
    )

    return Event(
        event_id=str(uuid.uuid4()),
        event_type=random.choice(EVENT_TYPES),
        event_ts=ts.isoformat(),
        customer_id=f"c{random.randint(1, 5000)}",
        order_id=f"o{random.randint(1, 20000)}",
        payload=Payload(
            sku=random.choice(SKUS),
            amount=round(random.uniform(10, 200), 2),
        ),
    )        


def make_file(day: datetime.datetime) -> str:
    count = random.randint(10,55)
    events = [make_event(day) for _ in range(count)]
    return json.dumps([asdict(event) for event in events], indent=2)

def random_name():
    filename = f"events_{uuid.uuid4().hex}" + random.choice([".json", ".data", ".txt"])
    return filename


BUCKET = os.environ.get("EVENTS_S3_BUCKET")
MIN_BYTES, MAX_BYTES = 2048, 16000

def write(key: str, data: str):
    if len(data.encode()) > MAX_BYTES or len(data.encode()) < MIN_BYTES:
        raise ValueError(f"File size {len(data.encode())} is not between {MIN_BYTES} and {MAX_BYTES} bytes")
    if BUCKET:
        s3 = boto3.client("s3")
        s3.put_object(Bucket=BUCKET, Key=key, Body=data.encode())
    else:
        path = os.path.join(OUT_DIR, key)                   # out/raw/events/dt=.../events_...
        os.makedirs(os.path.dirname(path), exist_ok=True)   # create out/raw/events/dt=.../
        with open(path, "w") as f:
            f.write(data)

def main():
    today = datetime.datetime.now(datetime.timezone.utc)
    for days_ago in range(LAST_DAYS):
        day = today - datetime.timedelta(days=days_ago)
        files = [make_file(day) for _ in range(FILES_PER_DAY)]
        files.append(random.choice(files))  # duplicate a random file to simulate duplicates
        files.append(make_file(today)[:-10 ]) # add corrupted file to simulate a corrupted file
        for file in files:
            filename = f"raw/events/dt={day:%Y-%m-%d}/{random_name()}"
            write(filename, file)

if __name__ == "__main__":
    main()