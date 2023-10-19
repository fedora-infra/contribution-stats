#!/usr/bin/env python3

# SPDX-FileCopyrightText: Contributors to the Fedora Project
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
import sqlite3
import statistics
from datetime import date, datetime, timedelta

import click


def for_each_month(start):
    today = date.today().replace(day=1)
    current_date = start.replace(day=1)
    while current_date <= today:
        yield current_date
        current_date = current_date + timedelta(days=32)
        current_date = current_date.replace(day=1)


def month_filter(month):
    return f"year = {month.year} AND month = {month.month}"


def orphaned(db, month):
    result = db.execute("SELECT COUNT(*) FROM orphaned WHERE " + month_filter(month))
    return result.fetchone()[0]


def orphaners(db, month):
    result = db.execute("SELECT COUNT(DISTINCT(by)) FROM orphaned WHERE " + month_filter(month))
    return result.fetchone()[0]


def adopted(db, month):
    result = db.execute("SELECT COUNT(*) FROM adopted WHERE " + month_filter(month))
    return result.fetchone()[0]


def adoption(db, month):
    adoption_times = []
    result = db.execute(
        "SELECT DISTINCT(package), timestamp FROM adopted WHERE " + month_filter(month)
    )
    for row in result:
        orphaned_result = db.execute(
            """
                SELECT timestamp FROM orphaned
                WHERE package = ? AND
                date(format('%i-%02i-01', year, month)) <= date(format('%i-%02i-01', ?, ?))
                ORDER BY year DESC, month DESC
        """,
            (row[0], month.year, month.month),
        )
        orphaned = orphaned_result.fetchone()
        if not orphaned:
            continue
        adoption_time = datetime.fromisoformat(row[1]) - datetime.fromisoformat(orphaned[0])
        adoption_times.append(adoption_time.days)
    return statistics.mean(adoption_times) if adoption_times else None


def adopters(db, month):
    result = db.execute("SELECT COUNT(DISTINCT(by)) FROM adopted WHERE " + month_filter(month))
    return result.fetchone()[0]


def retired(db, month):
    result = db.execute(
        """SELECT COUNT(DISTINCT(o.package)) FROM orphaned o
        JOIN retired r ON o.package = r.package
        WHERE o.year = ? AND o.month = ?""",
        (month.year, month.month),
    )
    return result.fetchone()[0]


def committed(db, month):
    result = db.execute("SELECT COUNT(DISTINCT(package)) FROM commits WHERE " + month_filter(month))
    return result.fetchone()[0]


def committers(db, month):
    result = db.execute("SELECT COUNT(DISTINCT(by)) FROM commits WHERE " + month_filter(month))
    return result.fetchone()[0]


@click.command()
@click.argument("input_file", type=click.Path(), required=True)
@click.argument("output_file", type=click.Path(), required=True)
def main(input_file, output_file):
    start = date(2020, 8, 1)
    connection = sqlite3.connect(input_file)
    with open(output_file, "w") as fh:
        csvwriter = csv.writer(fh)
        csvwriter.writerow(
            [
                "Date",
                "Orphaned",
                "Orphaners",
                "Retired",
                "Adoptions",
                "Adopters",
                "Avg adoption days",
                "Packages with commits",
                "Committers",
            ]
        )
        for month in for_each_month(start):
            avg_adoption_time = adoption(connection, month)
            csvwriter.writerow(
                [
                    month.strftime("%Y-%m"),
                    orphaned(connection, month),
                    orphaners(connection, month),
                    retired(connection, month),
                    adopted(connection, month),
                    adopters(connection, month),
                    f"{avg_adoption_time:.02f}" if avg_adoption_time else "",
                    committed(connection, month),
                    committers(connection, month),
                ]
            )


if __name__ == "__main__":
    main()
