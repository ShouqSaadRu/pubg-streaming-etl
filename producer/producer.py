from __future__ import annotations
import hashlib, json, os, time
from datetime import datetime
from typing import Any
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer.api_client import PubgApiClient, PubgApiError

load_dotenv()
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_COMBAT_TOPIC", "pubg.combat-events")
API_KEY = os.getenv("PUBG_API_KEY", "")
SHARD = os.getenv("PUBG_SHARD", "steam")
REPLAY_SPEED = float(os.getenv("PUBG_REPLAY_SPEED", "20"))

def delivery_callback(error, message) -> None:
    if error:
        print(f"Delivery failed: {error}")
        return
    print(f"Delivered | partition={message.partition()} offset={message.offset()}")

def character_name(character: dict[str, Any] | None) -> str:
    return "Environment" if not character else (character.get("name") or "Unknown")

def build_event_id(match_id: str, telemetry_index: int, raw_event: dict[str, Any]) -> str:
    identity = f"{match_id}|{telemetry_index}|{raw_event.get('_T')}|{raw_event.get('_D')}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()

def transform_kill_event(raw_event: dict[str, Any], match_id: str, telemetry_index: int) -> dict[str, Any]:
    killer = raw_event.get("killer")
    victim = raw_event.get("victim") or {}
    damage_info = raw_event.get("killerDamageInfo") or raw_event.get("finishDamageInfo") or raw_event.get("dBNODamageInfo") or {}
    return {
        "event_id": build_event_id(match_id, telemetry_index, raw_event),
        "event_type": raw_event.get("_T"),
        "event_time": raw_event.get("_D"),
        "match_id": match_id,
        "killer": character_name(killer),
        "killer_account_id": killer.get("accountId") if killer else None,
        "killer_team_id": killer.get("teamId") if killer else None,
        "victim": character_name(victim),
        "victim_account_id": victim.get("accountId"),
        "victim_team_id": victim.get("teamId"),
        "weapon_raw": damage_info.get("damageCauserName") or "Unknown",
        "damage_reason_raw": damage_info.get("damageReason"),
        "damage_type_raw": damage_info.get("damageTypeCategory"),
        "distance_raw": round(float(damage_info.get("distance") or 0), 2),
        "is_headshot": damage_info.get("damageReason") == "HeadShot",
        "is_suicide": bool(raw_event.get("isSuicide")),
        "assist_account_ids": raw_event.get("assists_AccountId", []),
    }

def parse_timestamp(value: str | None) -> datetime | None:
    return None if not value else datetime.fromisoformat(value.replace("Z", "+00:00"))

def replay_delay(previous_time: datetime | None, current_time: datetime | None) -> None:
    if REPLAY_SPEED <= 0 or previous_time is None or current_time is None:
        return
    delay = (current_time - previous_time).total_seconds()
    if delay > 0:
        time.sleep(min(delay / REPLAY_SPEED, 3))

def main() -> None:
    api = PubgApiClient(API_KEY, SHARD)

    print(f'Getting a random match from shard "{SHARD}"...')

    match_id = api.get_random_match_id()

    print(f"Selected match: {match_id}")

    match = api.get_match(match_id)

    players = api.get_match_players(match)

    print(f"Players found: {len(players)}")

    for player in players[:10]:
        print(
            f"{player['name']} | "
            f"kills={player['kills']} | "
            f"placement={player['win_place']}"
        )

    telemetry_url = api.get_telemetry_url(match)

    print("Downloading telemetry...")

    telemetry = api.get_telemetry(telemetry_url)

    kills = [
        (index, event)
        for index, event in enumerate(telemetry)
        if event.get("_T") == "LogPlayerKillV2"
    ]

    print(
        f"Telemetry events: {len(telemetry)} | "
        f"Kill events: {len(kills)}"
    )

    if not kills:
        print("No LogPlayerKillV2 events were found in this match.")
        return

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "pubg-api-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    previous_event_time = None

    for telemetry_index, raw_event in kills:
        event = transform_kill_event(
            raw_event,
            match_id,
            telemetry_index,
        )

        current_event_time = parse_timestamp(
            event["event_time"]
        )

        replay_delay(
            previous_event_time,
            current_event_time,
        )

        print(
            f"{event['killer']} killed "
            f"{event['victim']} with "
            f"{event['weapon_raw']} from "
            f"{event['distance_raw']}cm"
        )

        producer.produce(
            topic=TOPIC,
            key=match_id,
            value=json.dumps(event),
            callback=delivery_callback,
        )

        producer.poll(0)
        previous_event_time = current_event_time

    remaining = producer.flush(30)

    if remaining:
        raise RuntimeError(
            f"{remaining} Kafka message(s) were not delivered."
        )

    print(
        f"Published {len(kills)} real kill events "
        f"to {TOPIC}."
    )


if __name__ == "__main__":
    try:
        main()
    except (
        PubgApiError,
        ValueError,
        RuntimeError,
    ) as exc:
        raise SystemExit(f"Error: {exc}") from exc

    