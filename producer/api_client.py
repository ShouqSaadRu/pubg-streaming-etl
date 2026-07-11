from __future__ import annotations

import random
from typing import Any
from urllib.parse import quote

import requests


class PubgApiError(RuntimeError):
    pass


class PubgApiClient:
    BASE_URL = "https://api.pubg.com"

    def __init__(
        self,
        api_key: str,
        shard: str = "steam",
    ) -> None:
        if (
            not api_key
            or api_key == "replace_with_your_pubg_api_key"
        ):
            raise ValueError(
                "Set PUBG_API_KEY in your .env file."
            )

        self.shard = shard

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/vnd.api+json",
                "Accept-Encoding": "gzip",
                "User-Agent": "pubg-streaming-platform/1.0",
            }
        )

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        use_auth: bool = True,
    ) -> Any:
        try:
            if use_auth:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=30,
                )
            else:
                response = requests.get(
                    url,
                    headers={
                        "Accept": "application/vnd.api+json",
                        "Accept-Encoding": "gzip",
                        "User-Agent": (
                            "pubg-streaming-platform/1.0"
                        ),
                    },
                    timeout=60,
                )

        except requests.RequestException as exc:
            raise PubgApiError(
                f"Request failed: {exc}"
            ) from exc

        if response.status_code == 404:
            raise PubgApiError(
                f"PUBG resource not found: {url}"
            )

        if response.status_code == 429:
            raise PubgApiError(
                "PUBG API rate limit reached. "
                "Try again after one minute."
            )

        try:
            response.raise_for_status()

        except requests.HTTPError as exc:
            raise PubgApiError(
                f"PUBG API returned HTTP "
                f"{response.status_code}: "
                f"{response.text[:300]}"
            ) from exc

        return response.json()

    def get_random_match_id(self) -> str:
        url = (
            f"{self.BASE_URL}/shards/"
            f"{self.shard}/samples"
        )

        payload = self._get_json(url)

        matches = (
            payload.get("data", {})
            .get("relationships", {})
            .get("matches", {})
            .get("data", [])
        )

        if not matches:
            raise PubgApiError(
                "The samples endpoint returned no matches."
            )

        selected_match = random.choice(matches)

        return selected_match["id"]

    def get_match(
        self,
        match_id: str,
    ) -> dict[str, Any]:
        encoded_match_id = quote(
            match_id,
            safe="",
        )

        url = (
            f"{self.BASE_URL}/shards/"
            f"{self.shard}/matches/"
            f"{encoded_match_id}"
        )

        return self._get_json(url)

    @staticmethod
    def get_match_players(
        match_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        players: list[dict[str, Any]] = []

        for item in match_payload.get("included", []):
            if item.get("type") != "participant":
                continue

            attributes = item.get("attributes", {})
            stats = attributes.get("stats", {})

            players.append(
                {
                    "participant_id": item.get("id"),
                    "player_id": stats.get("playerId"),
                    "name": stats.get("name"),
                    "kills": stats.get("kills"),
                    "damage_dealt": stats.get(
                        "damageDealt"
                    ),
                    "win_place": stats.get(
                        "winPlace"
                    ),
                    "time_survived": stats.get(
                        "timeSurvived"
                    ),
                }
            )

        return players

    @staticmethod
    def get_telemetry_url(
        match_payload: dict[str, Any],
    ) -> str:
        asset_references = (
            match_payload.get("data", {})
            .get("relationships", {})
            .get("assets", {})
            .get("data", [])
        )

        if not asset_references:
            raise PubgApiError(
                "Match response contains no "
                "telemetry asset."
            )

        asset_id = asset_references[0]["id"]

        for item in match_payload.get("included", []):
            if (
                item.get("type") == "asset"
                and item.get("id") == asset_id
            ):
                telemetry_url = (
                    item.get("attributes", {})
                    .get("URL")
                )

                if telemetry_url:
                    return telemetry_url

        raise PubgApiError(
            "Telemetry asset was referenced, "
            "but its URL was not found."
        )

    def get_telemetry(
        self,
        telemetry_url: str,
    ) -> list[dict[str, Any]]:
        payload = self._get_json(
            telemetry_url,
            use_auth=False,
        )

        if not isinstance(payload, list):
            raise PubgApiError(
                "Telemetry response was not "
                "a JSON array."
            )

        return payload