from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, render_template, request


BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
ENV_PATH = BASE_DIR / ".env"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CACHE_TTL_SECONDS = 24 * 60 * 60
CATALOG_CACHE_TTL_SECONDS = 30 * 24 * 60 * 60
REQUEST_TIMEOUT_SECONDS = 30
HISTORY_SAMPLE_SIZE = 2
LISTING_SAMPLE_SIZE = 40
SEGMENTED_HISTORY_REQUEST_LIMIT = 20
SEGMENTED_BUCKET_ENRICH_LIMIT = 12
MARKETCHECK_BASE_URL = "https://api.marketcheck.com"
MARKETCHECK_CACHE_VERSION = "marketcheck-v7"
ALL_TRIMS_LABEL = "All Trims"
PREFERRED_MAKES = [
    "Tesla",
    "Honda",
    "Toyota",
    "Acura",
    "Ford",
    "Hyundai",
    "Kia",
    "Chevrolet",
    "Nissan",
    "Volkswagen",
    "Rivian",
    "Lucid",
    "Polestar",
    "Genesis",
    "BMW",
    "Audi",
    "Porsche",
    "Mercedes-Benz",
]
CATALOG_RESPONSE_TTL_SECONDS = 30 * 24 * 60 * 60


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class VehicleSelection:
    vehicle_year: Optional[int]
    manufacturer: str
    model: str
    trim: str = ALL_TRIMS_LABEL

    @property
    def label(self) -> str:
        year_label = self.vehicle_year if self.vehicle_year is not None else "All Years"
        if self.trim == ALL_TRIMS_LABEL:
            return f"{year_label} {self.manufacturer} {self.model}"
        return f"{year_label} {self.manufacturer} {self.model} {self.trim}"

    @property
    def slug(self) -> str:
        year_slug = self.vehicle_year if self.vehicle_year is not None else "all"
        value = f"{year_slug}-{self.manufacturer}-{self.model}-{self.trim}".lower()
        return re.sub(r"[^a-z0-9]+", "-", value).strip("-")


class MarketCheckProvider:
    name = "MarketCheck"
    series_kind = "market_listing_trends"
    source_limitations = (
        "Charts are based on MarketCheck listing history and market stats, not a nationwide public "
        "confirmed-sale ledger."
    )

    def __init__(self) -> None:
        self.api_key = os.getenv("MARKETCHECK_API_KEY", "").strip()
        self.api_secret = os.getenv("MARKETCHECK_API_SECRET", "").strip()
        self.base_url = os.getenv("MARKETCHECK_BASE_URL", MARKETCHECK_BASE_URL).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def provider_status(self) -> Dict[str, Any]:
        return {
            "provider": self.name,
            "configured": self.configured,
            "using_fallback": False,
            "preferred_provider": self.name,
            "preferred_provider_configured": self.configured,
            "series_kind": self.series_kind,
            "message": (
                self.source_limitations
                if self.configured
                else "MarketCheck credentials are not configured. Set MARKETCHECK_API_KEY to enable the app."
            ),
        }

    def catalog_years(self) -> List[int]:
        current_year = date.today().year
        return list(range(current_year, 2015, -1))

    def catalog_makes(self, vehicle_year: Optional[int]) -> List[str]:
        year_slug = vehicle_year if vehicle_year is not None else "all"
        cache_key = f"{MARKETCHECK_CACHE_VERSION}-terms-make-{year_slug}"
        params: Dict[str, Any] = {"field": "make"}
        if vehicle_year is not None:
            params["year"] = vehicle_year
        response = self._cached_request_json(
            cache_key,
            "/v2/specs/car/terms",
            params,
            ttl_seconds=CATALOG_CACHE_TTL_SECONDS,
        )
        values = response.get("make", [])
        return merge_preferred_values(values, PREFERRED_MAKES)

    def catalog_models(self, vehicle_year: Optional[int], make: str) -> List[str]:
        year_slug = vehicle_year if vehicle_year is not None else "all"
        cache_key = f"{MARKETCHECK_CACHE_VERSION}-terms-model-{year_slug}-{slugify(make)}"
        params: Dict[str, Any] = {"field": "model", "make": make}
        if vehicle_year is not None:
            params["year"] = vehicle_year
        response = self._cached_request_json(
            cache_key,
            "/v2/specs/car/terms",
            params,
            ttl_seconds=CATALOG_CACHE_TTL_SECONDS,
        )
        return response.get("model", [])

    def catalog_trims(self, vehicle_year: Optional[int], make: str, model: str) -> List[str]:
        year_slug = vehicle_year if vehicle_year is not None else "all"
        cache_key = f"{MARKETCHECK_CACHE_VERSION}-terms-trim-{year_slug}-{slugify(make)}-{slugify(model)}"
        params: Dict[str, Any] = {"field": "trim", "make": make, "model": model}
        if vehicle_year is not None:
            params["year"] = vehicle_year
        response = self._cached_request_json(
            cache_key,
            "/v2/specs/car/terms",
            params,
            ttl_seconds=CATALOG_CACHE_TTL_SECONDS,
        )
        trims = response.get("trim", [])
        return [ALL_TRIMS_LABEL, *trims]

    def fetch_vehicle_series(
        self, selection: VehicleSelection, start_year: int, last_month_end: date
    ) -> Dict[str, Any]:
        cache_path = CACHE_DIR / f"{MARKETCHECK_CACHE_VERSION}-{selection.slug}-{start_year}.json"
        cached = load_json_cache(cache_path)
        if cached is not None:
            return cached

        listings = self._fetch_matching_listings(selection)
        if not listings:
            raise ValueError(f"No MarketCheck listings found for {selection.label}.")

        history_points = self._fetch_listing_history_points(listings, selection, start_year, last_month_end)
        if not history_points:
            raise ValueError(
                f"No MarketCheck listing history points found for {selection.label} "
                f"from {start_year} through {last_month_end.isoformat()}."
            )

        monthly = aggregate_price_points(history_points, "month")
        yearly = aggregate_rows(monthly, "month", "year")
        segmented_monthly = aggregate_segmented_price_points(history_points, "month")
        segmented_yearly = aggregate_segmented_rows(segmented_monthly, "month", "year")
        segmented_monthly, segmented_yearly = self._enrich_segmented_series(
            selection,
            segmented_monthly,
            segmented_yearly,
            start_year,
            last_month_end,
        )
        first_average = monthly[0]["average_price"]
        latest_average = monthly[-1]["average_price"]
        market_stats = self._fetch_market_stats(selection)
        mean_price = market_stats.get("mean_price")
        if mean_price is None:
            mean_price = average_price(monthly)

        payload = {
            "selection": selection_to_dict(selection),
            "monthly": monthly,
            "yearly": yearly,
            "segmented_monthly": segmented_monthly,
            "segmented_yearly": segmented_yearly,
            "summary": {
                "average_price": round(mean_price, 2),
                "depreciation_pct": round(((latest_average - first_average) / first_average) * 100, 2),
                "coverage_label": "Trend Points",
                "coverage_value": len(history_points),
                "points_count": len(monthly),
                "market_count": market_stats.get("count", 0),
                "first_month_average": round(first_average, 2),
                "latest_month_average": round(latest_average, 2),
            },
            "records_title": "Recent Market Trend Points",
            "records": recent_record_points(history_points),
        }
        write_json_cache(cache_path, payload)
        return payload

    def _fetch_matching_listings(self, selection: VehicleSelection) -> List[Dict[str, Any]]:
        cache_path = CACHE_DIR / f"{MARKETCHECK_CACHE_VERSION}-active-{selection.slug}.json"
        cached = load_json_cache(cache_path)
        if cached is not None:
            listings = cached.get("listings", [])
            if selection.trim == ALL_TRIMS_LABEL:
                return listings
            return [listing for listing in listings if trim_matches_listing(selection.trim, listing)]

        params: Dict[str, Any] = {
            "make": selection.manufacturer,
            "model": selection.model,
            "rows": LISTING_SAMPLE_SIZE,
            "sort_by": "last_seen_at",
            "sort_order": "desc",
        }
        if selection.vehicle_year is not None:
            params["year"] = selection.vehicle_year
        if selection.trim != ALL_TRIMS_LABEL:
            params["trim"] = selection.trim

        response = self._request_json("/v2/search/car/active", params)
        listings = response.get("listings", [])
        write_json_cache(cache_path, {"listings": listings})
        if selection.trim == ALL_TRIMS_LABEL:
            return listings
        return [listing for listing in listings if trim_matches_listing(selection.trim, listing)]

    def _fetch_listing_history_points(
        self,
        listings: List[Dict[str, Any]],
        selection: VehicleSelection,
        start_year: int,
        last_month_end: date,
    ) -> List[Dict[str, Any]]:
        start_date = date(start_year, 1, 1)
        points: List[Dict[str, Any]] = []
        for listing in self._history_sample_listings(listings, selection):
            history = self._load_listing_history(listing)
            if history is None:
                break
            listing_vehicle_year = extract_vehicle_year(listing, selection.vehicle_year)
            listing_trim = extract_trim(listing, selection.trim)
            for entry in history:
                point = normalize_history_point(
                    entry,
                    selection,
                    start_date,
                    last_month_end,
                    listing_vehicle_year,
                    listing_trim,
                )
                if point is not None:
                    points.append(point)

        deduped: Dict[tuple[str, str], Dict[str, Any]] = {}
        for point in points:
            deduped[(point["date"].isoformat(), point["listing_url"] or point["title"])] = point

        for listing in listings:
            point = normalize_listing_point(listing, selection, start_date, last_month_end)
            if point is not None:
                deduped[(point["date"].isoformat(), point["listing_url"] or point["title"])] = point

        return list(deduped.values())

    def _enrich_segmented_series(
        self,
        selection: VehicleSelection,
        segmented_monthly: List[Dict[str, Any]],
        segmented_yearly: List[Dict[str, Any]],
        start_year: int,
        last_month_end: date,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        should_enrich = selection.vehicle_year is None or selection.trim == ALL_TRIMS_LABEL
        if not should_enrich or not segmented_monthly:
            return segmented_monthly, segmented_yearly

        fallback_yearly = {
            (item["vehicle_year"], item["trim"]): item
            for item in segmented_yearly
        }
        enriched_monthly: List[Dict[str, Any]] = []
        enriched_yearly: List[Dict[str, Any]] = []

        for index, item in enumerate(segmented_monthly):
            bucket_key = (item["vehicle_year"], item["trim"])
            fallback_year_item = fallback_yearly.get(bucket_key, {"vehicle_year": item["vehicle_year"], "trim": item["trim"], "rows": []})
            if index >= SEGMENTED_BUCKET_ENRICH_LIMIT or item["trim"] == "Unknown Trim":
                enriched_monthly.append(item)
                enriched_yearly.append(fallback_year_item)
                continue

            bucket_selection = VehicleSelection(
                vehicle_year=item["vehicle_year"],
                manufacturer=selection.manufacturer,
                model=selection.model,
                trim=item["trim"],
            )
            try:
                bucket_payload = self.fetch_vehicle_series(bucket_selection, start_year, last_month_end)
                enriched_monthly.append(
                    {
                        "vehicle_year": item["vehicle_year"],
                        "trim": item["trim"],
                        "rows": bucket_payload["monthly"],
                    }
                )
                enriched_yearly.append(
                    {
                        "vehicle_year": item["vehicle_year"],
                        "trim": item["trim"],
                        "rows": bucket_payload["yearly"],
                    }
                )
            except (ValueError, requests.HTTPError):
                enriched_monthly.append(item)
                enriched_yearly.append(fallback_year_item)

        return enriched_monthly, enriched_yearly

    def _history_sample_listings(
        self,
        listings: List[Dict[str, Any]],
        selection: VehicleSelection,
    ) -> List[Dict[str, Any]]:
        if selection.vehicle_year is not None and selection.trim != ALL_TRIMS_LABEL:
            return dedupe_listings_by_vin(listings[:HISTORY_SAMPLE_SIZE])

        grouped: Dict[tuple[Optional[int], str], List[Dict[str, Any]]] = {}
        for listing in listings:
            bucket = (
                extract_vehicle_year(listing, selection.vehicle_year),
                extract_trim(listing, selection.trim) or "Unknown Trim",
            )
            grouped.setdefault(bucket, []).append(listing)

        sampled: List[Dict[str, Any]] = []
        ordered_groups = [(bucket, dedupe_listings_by_vin(bucket_listings)) for bucket, bucket_listings in sorted(grouped.items())]

        for _, bucket_listings in ordered_groups:
            if len(sampled) >= SEGMENTED_HISTORY_REQUEST_LIMIT:
                break
            sampled.append(preferred_history_listing(bucket_listings))

        if len(sampled) < SEGMENTED_HISTORY_REQUEST_LIMIT:
            for _, bucket_listings in sorted(ordered_groups, key=lambda item: len(item[1]), reverse=True):
                extras = [item for item in bucket_listings if item not in sampled]
                if not extras:
                    continue
                sampled.append(preferred_history_listing(extras))
                if len(sampled) >= SEGMENTED_HISTORY_REQUEST_LIMIT:
                    break
        return dedupe_listings_by_vin(sampled)

    def _load_listing_history(self, listing: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        vin = listing.get("vin")
        if not vin:
            return []
        cache_path = CACHE_DIR / f"{MARKETCHECK_CACHE_VERSION}-history-{vin}.json"
        cached_history = load_json_cache(cache_path)
        if cached_history is not None:
            return cached_history.get("items", [])
        try:
            history = self._request_json(f"/v2/history/car/{vin}", {}, expect_list=True)
            write_json_cache(cache_path, {"items": history})
            return history
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                return None
            raise

    def _fetch_market_stats(self, selection: VehicleSelection) -> Dict[str, Any]:
        cache_path = CACHE_DIR / f"{MARKETCHECK_CACHE_VERSION}-sales-{selection.slug}.json"
        cached = load_json_cache(cache_path)
        if cached is not None:
            return cached

        params: Dict[str, Any] = {
            "make": selection.manufacturer,
            "model": selection.model,
            "car_type": "used",
        }
        if selection.vehicle_year is not None:
            params["year"] = selection.vehicle_year
        if selection.trim != ALL_TRIMS_LABEL:
            params["trim"] = selection.trim
        try:
            response = self._request_json("/v2/sales/car", params)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                return {"count": 0, "mean_price": None}
            raise
        price_stats = response.get("price_stats", {})
        payload = {"count": response.get("count", 0), "mean_price": price_stats.get("mean")}
        write_json_cache(cache_path, payload)
        return payload

    def _request_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        expect_list: bool = False,
    ) -> Any:
        if not self.configured:
            raise ValueError("MarketCheck credentials are not configured.")

        query = dict(params or {})
        query["api_key"] = self.api_key
        response = self.session.get(
            f"{self.base_url}{path}",
            params=query,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        if expect_list:
            return data if isinstance(data, list) else []
        return data

    def _cached_request_json(
        self,
        cache_key: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = CACHE_TTL_SECONDS,
    ) -> Dict[str, Any]:
        cache_path = CACHE_DIR / f"{cache_key}.json"
        cached = load_json_cache(cache_path, ttl_seconds=ttl_seconds)
        if cached is not None:
            return cached

        try:
            payload = self._request_json(path, params)
        except requests.HTTPError as exc:
            stale = load_json_cache(cache_path, allow_stale=True, ttl_seconds=ttl_seconds)
            if stale is not None:
                return stale
            raise exc

        write_json_cache(cache_path, payload)
        return payload


def selection_to_dict(selection: VehicleSelection) -> Dict[str, Any]:
    return {
        "vehicle_year": selection.vehicle_year,
        "vehicle_year_label": selection.vehicle_year if selection.vehicle_year is not None else "All Years",
        "manufacturer": selection.manufacturer,
        "model": selection.model,
        "trim": selection.trim,
        "label": selection.label,
    }


def trim_matches_listing(trim: str, listing: Dict[str, Any]) -> bool:
    normalized_target = normalize_text(trim)
    heading = normalize_text(str(listing.get("heading", "")))
    build = normalize_text(str(listing.get("build", {}).get("trim", "")))
    return normalized_target in heading or normalized_target in build


def normalize_history_point(
    entry: Dict[str, Any],
    selection: VehicleSelection,
    start_date: date,
    last_month_end: date,
    vehicle_year: Optional[int],
    trim_label: Optional[str],
) -> Optional[Dict[str, Any]]:
    if selection.trim != ALL_TRIMS_LABEL and not trim_matches_listing(selection.trim, entry):
        return None
    price = entry.get("price")
    observed_at = parse_marketcheck_date(entry.get("first_seen_at_date")) or parse_marketcheck_date(
        entry.get("last_seen_at_date")
    )
    normalized_year = extract_vehicle_year(entry, vehicle_year)
    normalized_trim = extract_trim(entry, trim_label or selection.trim)
    if price is None or observed_at is None or normalized_year is None or normalized_trim is None:
        return None
    if not (start_date <= observed_at <= last_month_end):
        return None
    return {
        "date": observed_at,
        "vehicle_year": normalized_year,
        "trim": normalized_trim,
        "price": float(price),
        "title": entry.get("heading") or selection.label,
        "listing_url": entry.get("vdp_url", ""),
    }


def normalize_listing_point(
    listing: Dict[str, Any],
    selection: VehicleSelection,
    start_date: date,
    last_month_end: date,
) -> Optional[Dict[str, Any]]:
    if selection.trim != ALL_TRIMS_LABEL and not trim_matches_listing(selection.trim, listing):
        return None
    price = listing.get("price")
    observed_at = parse_marketcheck_date(listing.get("first_seen_at_date"))
    vehicle_year = extract_vehicle_year(listing, selection.vehicle_year)
    trim_label = extract_trim(listing, selection.trim)
    if price is None or observed_at is None or vehicle_year is None or trim_label is None:
        return None
    if not (start_date <= observed_at <= last_month_end):
        return None
    return {
        "date": observed_at,
        "vehicle_year": vehicle_year,
        "trim": trim_label,
        "price": float(price),
        "title": listing.get("heading") or selection.label,
        "listing_url": listing.get("vdp_url", ""),
    }


def parse_marketcheck_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def aggregate_price_points(points: List[Dict[str, Any]], grain: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[float]] = {}
    counts: Dict[str, int] = {}
    for point in points:
        key = point["date"].strftime("%Y-%m") if grain == "month" else point["date"].strftime("%Y")
        grouped.setdefault(key, []).append(point["price"])
        counts[key] = counts.get(key, 0) + 1
    return [
        {
            grain: period,
            "average_price": round(sum(values) / len(values), 2),
            "sales_count": counts[period],
        }
        for period, values in sorted(grouped.items())
    ]


def aggregate_rows(rows: List[Dict[str, Any]], source_key: str, target_key: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[float]] = {}
    counts: Dict[str, int] = {}
    for row in rows:
        period = row[source_key][:4] if target_key == "year" else row[source_key]
        grouped.setdefault(period, []).append(float(row["average_price"]))
        counts[period] = counts.get(period, 0) + int(row.get("sales_count", 0))
    return [
        {
            target_key: period,
            "average_price": round(sum(values) / len(values), 2),
            "sales_count": counts[period],
        }
        for period, values in sorted(grouped.items())
    ]


def aggregate_segmented_price_points(points: List[Dict[str, Any]], grain: str) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[int, str], List[Dict[str, Any]]] = {}
    for point in points:
        vehicle_year = point.get("vehicle_year")
        trim_label = point.get("trim")
        if vehicle_year is None or trim_label is None:
            continue
        grouped.setdefault((int(vehicle_year), str(trim_label)), []).append(point)
    return [
        {
            "vehicle_year": vehicle_year,
            "trim": trim_label,
            "rows": aggregate_price_points(year_points, grain),
        }
        for (vehicle_year, trim_label), year_points in sorted(grouped.items())
    ]


def aggregate_segmented_rows(
    grouped_rows: List[Dict[str, Any]],
    source_key: str,
    target_key: str,
) -> List[Dict[str, Any]]:
    return [
        {
            "vehicle_year": item["vehicle_year"],
            "trim": item["trim"],
            "rows": aggregate_rows(item["rows"], source_key, target_key),
        }
        for item in grouped_rows
    ]


def average_price(rows: List[Dict[str, Any]]) -> float:
    return sum(float(row["average_price"]) for row in rows) / len(rows)


def merge_preferred_values(values: List[str], preferred: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in preferred + sorted(values):
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def recent_record_points(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(points, key=lambda item: item["date"])
    return [
        {
            "sale_date": point["date"].isoformat(),
            "title": point["title"],
            "sale_price": round(point["price"], 2),
            "listing_url": point["listing_url"],
        }
        for point in ordered[-10:]
    ]


def dedupe_listings_by_vin(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    result = []
    for listing in listings:
        vin = listing.get("vin")
        key = vin or listing.get("id")
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(listing)
    return result


def preferred_history_listing(listings: List[Dict[str, Any]]) -> Dict[str, Any]:
    cached_listing = next((item for item in listings if has_cached_history(item)), None)
    if cached_listing is not None:
        return cached_listing
    return listings[0]


def build_depreciation_payload(primary_payload: Dict[str, Any], comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    def normalize(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
        base = rows[0]["average_price"]
        return [
            {
                key: row[key],
                "value_index": round((row["average_price"] / base) * 100, 2),
            }
            for row in rows
        ]

    return {
        "selection": comparison_payload["selection"],
        "monthly_primary": normalize(primary_payload["monthly"], "month"),
        "monthly_comparison": normalize(comparison_payload["monthly"], "month"),
        "yearly_primary": normalize(primary_payload["yearly"], "year"),
        "yearly_comparison": normalize(comparison_payload["yearly"], "year"),
    }


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def extract_vehicle_year(payload: Dict[str, Any], fallback: Optional[int] = None) -> Optional[int]:
    candidates = [
        payload.get("year"),
        payload.get("build", {}).get("year") if isinstance(payload.get("build"), dict) else None,
        fallback,
    ]
    for candidate in candidates:
        try:
            if candidate is None or candidate == "":
                continue
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def extract_trim(payload: Dict[str, Any], fallback: Optional[str] = None) -> Optional[str]:
    candidates = [
        payload.get("trim"),
        payload.get("build", {}).get("trim") if isinstance(payload.get("build"), dict) else None,
        fallback,
    ]
    for candidate in candidates:
        value = str(candidate or "").strip()
        if not value:
            continue
        if value == ALL_TRIMS_LABEL:
            continue
        return value
    if fallback == ALL_TRIMS_LABEL:
        return "Unknown Trim"
    return str(fallback or "").strip() or None


def has_cached_history(listing: Dict[str, Any]) -> bool:
    vin = listing.get("vin")
    if not vin:
        return False
    cache_path = CACHE_DIR / f"{MARKETCHECK_CACHE_VERSION}-history-{vin}.json"
    return cache_path.exists()


def load_json_cache(
    path: Path,
    allow_stale: bool = False,
    ttl_seconds: int = CACHE_TTL_SECONDS,
) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    if not allow_stale and time.time() - path.stat().st_mtime > ttl_seconds:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_cache(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_vehicle_selection(payload: Optional[Dict[str, Any]]) -> VehicleSelection:
    if not payload:
        raise ValueError("Vehicle selection is required.")
    raw_vehicle_year = payload.get("vehicle_year")
    if raw_vehicle_year in (None, "", "all", "All Years"):
        vehicle_year = None
    else:
        try:
            vehicle_year = int(raw_vehicle_year)
        except (TypeError, ValueError) as exc:
            raise ValueError("vehicle_year must be an integer or 'all'.") from exc

    manufacturer = str(payload.get("manufacturer", "")).strip()
    model = str(payload.get("model", "")).strip()
    trim = str(payload.get("trim", ALL_TRIMS_LABEL)).strip() or ALL_TRIMS_LABEL
    if not manufacturer or not model:
        raise ValueError("manufacturer and model are required.")
    return VehicleSelection(vehicle_year=vehicle_year, manufacturer=manufacturer, model=model, trim=trim)


def get_last_month_end(today: Optional[date] = None) -> date:
    today = today or date.today()
    return today.replace(day=1) - timedelta(days=1)


def parse_vehicle_year_arg(value: str) -> Optional[int]:
    cleaned = value.strip().lower()
    if not cleaned or cleaned == "all":
        return None
    try:
        return int(cleaned)
    except ValueError as exc:
        raise ValueError("vehicle_year must be an integer or 'all'.") from exc


def build_boot_catalog(provider: MarketCheckProvider) -> Dict[str, Any]:
    years = provider.catalog_years()
    default_vehicle_year = 2023 if 2023 in years else years[0]
    fallback_models = ["Model 3", "Model Y"]
    fallback_trims = {
        "Model 3": [ALL_TRIMS_LABEL, "Long Range", "Performance", "Base"],
        "Model Y": [ALL_TRIMS_LABEL, "Long Range", "Performance", "Base"],
    }
    payload: Dict[str, Any] = {
        "years": years,
        "default_vehicle_year": default_vehicle_year,
        "makes": merge_preferred_values([], PREFERRED_MAKES),
        "tesla_models": fallback_models,
        "tesla_trims": fallback_trims,
    }
    if not provider.configured:
        return payload

    try:
        makes = provider.catalog_makes(default_vehicle_year)
        if makes:
            payload["makes"] = makes
    except requests.RequestException:
        return payload

    if "Tesla" not in payload["makes"]:
        return payload

    try:
        tesla_models = provider.catalog_models(default_vehicle_year, "Tesla")
        if tesla_models:
            payload["tesla_models"] = tesla_models
    except requests.RequestException:
        return payload

    tesla_trims = dict(fallback_trims)
    for model_name in payload["tesla_models"]:
        try:
            trims = provider.catalog_trims(default_vehicle_year, "Tesla", model_name)
        except requests.RequestException:
            continue
        if trims:
            tesla_trims[model_name] = trims
    payload["tesla_trims"] = tesla_trims
    return payload


def json_with_cache(payload: Dict[str, Any], max_age: int) -> Any:
    response = jsonify(payload)
    response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return response


def create_app() -> Flask:
    app = Flask(__name__)
    provider = MarketCheckProvider()

    @app.get("/")
    def index():
        boot_catalog = build_boot_catalog(provider)
        return render_template("index.html", years=boot_catalog["years"], boot_catalog=boot_catalog)

    @app.get("/api/provider")
    def provider_status():
        return jsonify(provider.provider_status())

    @app.get("/api/catalog/years")
    def catalog_years():
        return json_with_cache(
            {"provider": provider.name, "items": provider.catalog_years()},
            CATALOG_RESPONSE_TTL_SECONDS,
        )

    @app.get("/api/catalog/makes")
    def catalog_makes():
        vehicle_year = parse_vehicle_year_arg(request.args.get("vehicle_year", ""))
        return json_with_cache(
            {"provider": provider.name, "items": provider.catalog_makes(vehicle_year)},
            CATALOG_RESPONSE_TTL_SECONDS,
        )

    @app.get("/api/catalog/models")
    def catalog_models():
        vehicle_year = parse_vehicle_year_arg(request.args.get("vehicle_year", ""))
        manufacturer = request.args.get("manufacturer", "").strip()
        if not manufacturer:
            raise ValueError("manufacturer is required.")
        return json_with_cache(
            {"provider": provider.name, "items": provider.catalog_models(vehicle_year, manufacturer)},
            CATALOG_RESPONSE_TTL_SECONDS,
        )

    @app.get("/api/catalog/trims")
    def catalog_trims():
        vehicle_year = parse_vehicle_year_arg(request.args.get("vehicle_year", ""))
        manufacturer = request.args.get("manufacturer", "").strip()
        model = request.args.get("model", "").strip()
        if not manufacturer or not model:
            raise ValueError("manufacturer and model are required.")
        return json_with_cache(
            {"provider": provider.name, "items": provider.catalog_trims(vehicle_year, manufacturer, model)},
            CATALOG_RESPONSE_TTL_SECONDS,
        )

    @app.post("/api/prices")
    def prices():
        payload = request.get_json(silent=True) or {}
        try:
            start_year = int(payload.get("start_year", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("start_year must be an integer.") from exc
        if start_year <= 0:
            raise ValueError("start_year is required.")

        primary = parse_vehicle_selection(payload.get("primary"))
        comparisons = [parse_vehicle_selection(item) for item in payload.get("comparisons", []) if item]
        last_month_end = get_last_month_end()

        primary_payload = provider.fetch_vehicle_series(primary, start_year, last_month_end)
        comparison_payloads = []
        depreciation_payloads = []
        for comparison in comparisons:
            comparison_payload = provider.fetch_vehicle_series(comparison, start_year, last_month_end)
            comparison_payloads.append(comparison_payload)
            depreciation_payloads.append(build_depreciation_payload(primary_payload, comparison_payload))

        return jsonify(
            {
                "provider": provider.provider_status(),
                "last_month_end": last_month_end.isoformat(),
                "primary": primary_payload,
                "comparisons": comparison_payloads,
                "depreciation": depreciation_payloads,
            }
        )

    @app.errorhandler(ValueError)
    def handle_value_error(error: ValueError):
        return jsonify({"error": str(error)}), 400

    @app.errorhandler(requests.HTTPError)
    def handle_http_error(error: requests.HTTPError):
        status_code = error.response.status_code if error.response is not None else 502
        if status_code == 429:
            message = (
                "MarketCheck rate limit exceeded. Wait a moment and try again, or reuse a cached "
                "selection that has already been loaded."
            )
        else:
            message = f"Upstream provider request failed with status {status_code}."
        return jsonify({"error": message}), status_code

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
