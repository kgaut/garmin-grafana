"""Backfill the extra ActivitySummary fields of activities already stored in InfluxDB 1.x.

Writes, on each ActivitySummary start point already in the database, the fields
built by garmin_fetch.get_activity_extra_fields (eventType, startTimeLocal, steps,
power, running dynamics, fastest splits...) and, with --details, the fields of
garmin_fetch.get_activity_details_fields (feel, RPE, min HR, time zone, event goal).
Each point is written with the exact tags and timestamp of the stored one, so
InfluxDB merges the new fields into it: no other field or measurement is rewritten,
no FIT file is downloaded, no point is created for an activity missing from the
database, and points already backfilled are skipped (startTimeLocal, plus timeZone
with --details).

Garmin requests: the activity list, year by year (one request per 20 activities),
plus one get_activity request per activity with --details (paced by --pause).

Usage (inside the garmin-fetch-data container):
    python garmin_grafana/activity_summary_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--details] [--dry-run]
"""
import garmin_fetch

import argparse
import logging
import time
from collections import Counter
from datetime import datetime, timedelta

import pytz
from garminconnect import GarminConnectTooManyRequestsError

from event_type_backfill import year_ranges


def stored_start_points(start_date, end_date):
    """Map ActivityID -> list of (time, tags, row) of the stored ActivitySummary points."""
    query = (
        'SELECT * FROM "ActivitySummary" '
        f"WHERE time >= '{start_date:%Y-%m-%d}T00:00:00Z' AND time < '{end_date + timedelta(days=1):%Y-%m-%d}T00:00:00Z' "
        'GROUP BY *'
    )
    points = {}
    for (_, tags), rows in garmin_fetch.influxdbclient.query(query).items():
        for row in rows:
            points.setdefault(str(tags.get('ActivityID')), []).append((row['time'], dict(tags), row))
    return points


def backfill(start_date, end_date, details=False, dry_run=False, pause_seconds=1.0):
    stats = Counter()
    for chunk_start, chunk_end in year_ranges(start_date, end_date):
        activities = garmin_fetch.garmin_obj.get_activities_by_date(f"{chunk_start:%Y-%m-%d}", f"{chunk_end:%Y-%m-%d}")
        stored = stored_start_points(chunk_start, chunk_end)
        points = []
        for activity in activities:
            stats['garmin_activities'] += 1
            if not activity.get('startTimeGMT'):
                stats['no_start_time'] += 1
                continue
            start_time = datetime.strptime(activity['startTimeGMT'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            matches = [p for p in stored.get(str(activity.get('activityId')), []) if p[0] == start_time_str]
            if not matches:
                stats['not_in_influxdb'] += 1
                continue
            todo = [p for p in matches if p[2].get('startTimeLocal') is None or (details and p[2].get('timeZone') is None)]
            if not todo:
                stats['already_backfilled'] += 1
                continue
            fields = garmin_fetch.get_activity_extra_fields(activity)
            if details and dry_run:
                stats['details_requests_planned'] += 1
            elif details:
                try:
                    fields.update(garmin_fetch.get_activity_details_fields(activity['activityId']))
                except GarminConnectTooManyRequestsError:
                    logging.error(f"Garmin rate limit reached: resume later with --start {chunk_start:%Y-%m-%d} (points of this year written so far are kept)")
                    if points and not dry_run:
                        garmin_fetch.write_points_v1_resolving_type_conflicts(points)
                    raise SystemExit(1)
                except Exception as err:
                    stats['details_failed'] += 1
                    logging.warning(f"Activity {activity.get('activityId')}: details not fetched ({err})")
                time.sleep(pause_seconds)
            fields = {k: v for k, v in fields.items() if v is not None}
            for point_time, tags, _ in todo:
                points.append({"measurement": "ActivitySummary", "time": point_time, "tags": tags, "fields": fields})
            stats['activities_to_update'] += 1
        stats['points_to_write'] += len(points)
        if points and not dry_run:
            garmin_fetch.write_points_v1_resolving_type_conflicts(points)
            stats['points_written'] += len(points)
        logging.info(f"{chunk_start:%Y-%m-%d} to {chunk_end:%Y-%m-%d}: {len(activities)} Garmin activities, {len(points)} points to update")
        time.sleep(5)
    logging.info(f"Summary{' (dry run, nothing written)' if dry_run else ''}: {dict(stats)}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill the extra ActivitySummary fields for activities already stored in InfluxDB 1.x")
    parser.add_argument("--start", required=True, help="first date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last date, YYYY-MM-DD (included)")
    parser.add_argument("--details", action="store_true", help="also fetch get_activity per activity (feel, RPE, min HR, time zone, event goal)")
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between two get_activity requests (default 1)")
    parser.add_argument("--dry-run", action="store_true", help="count what would be written, write nothing (no get_activity request)")
    args = parser.parse_args()

    if garmin_fetch.INFLUXDB_VERSION != '1':
        raise SystemExit("activity_summary_backfill only supports InfluxDB 1.x")
    garmin_fetch.garmin_obj = garmin_fetch.garmin_login()
    backfill(datetime.strptime(args.start, "%Y-%m-%d"), datetime.strptime(args.end, "%Y-%m-%d"),
             details=args.details, dry_run=args.dry_run, pause_seconds=args.pause)
