# PUBG Streaming Platform — Real API Producer

```text
PUBG API → Python Producer → Kafka → Python Consumer
```

## Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
PUBG_API_KEY=your_real_api_key
PUBG_SHARD=steam
PUBG_PLAYER_NAME=your_exact_pubg_name
PUBG_REPLAY_SPEED=20
```

`PUBG_REPLAY_SPEED=20` means twenty seconds of match time are replayed in approximately one real second. Use `0` to publish immediately.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d
chmod +x scripts/create-topics.sh
./scripts/create-topics.sh
```

Terminal 1:

```bash
python -m consumer.consumer
```

Terminal 2:

```bash
python -m producer.producer
```
