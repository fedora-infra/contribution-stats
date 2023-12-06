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


def _show_date(month):
    if month is None:
        return None
    return month.strftime("%Y-%m")


def month_filter(month):
    return f"year = {month.year} AND month = {month.month}"


def _get_months_left(db, month):
    result = db.execute(
        "SELECT COUNT(DISTINCT(year || '-' || month)) FROM commits WHERE timestamp > ?",
        (month.strftime("%Y-%m-%d"),),
    )
    return result.fetchone()[0] - 1


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


def _committers_in_future_months(db, month, committers):
    next_month = month + timedelta(days=31)
    next_month = next_month.replace(day=1)
    result = db.execute(
        f"SELECT \"by\" FROM commits WHERE \"by\" IN ({','.join('?' for _ in committers)}) "
        'AND timestamp >= ? group by "by"',
        [*committers, next_month.strftime("%Y-%m-%d")],
    )
    return [r[0] for r in result]


def orphaners_gone(db, month):
    if _get_months_left(db, month) <= 3:
        return None
    result = db.execute("SELECT DISTINCT(by) FROM orphaned WHERE " + month_filter(month))
    orphaners_this_month = [r[0] for r in result]
    committers_in_future_months = _committers_in_future_months(db, month, orphaners_this_month)
    gone = set(orphaners_this_month) - set(committers_in_future_months)
    return len(gone)


def committers_gone(db, month):
    if _get_months_left(db, month) <= 3:
        return None
    result = db.execute("SELECT DISTINCT(by) FROM commits WHERE " + month_filter(month))
    committers_this_month = [r[0] for r in result]
    committers_in_future_months = _committers_in_future_months(db, month, committers_this_month)
    gone = set(committers_this_month) - set(committers_in_future_months)
    # print(month, list(sorted(list(c_gone))))
    # next_month = month + timedelta(days=31)
    # next_month = next_month.replace(day=1)
    # c_gone = []
    # for committer in committers_this_month:
    #     result = db.execute(
    #         "SELECT COUNT(*) FROM commits WHERE \"by\" = ? AND timestamp >= ?",
    #         (committer, next_month.strftime("%Y-%m-%d"))
    #     )
    #     amount = result.fetchone()[0]
    #     if amount == 0:
    #         c_gone.append(committer)
    # print(month, list(sorted(c_gone)))
    return len(gone)


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
                "Orphaners who left",
                "Committers who left",
            ]
        )
        all_months = list(for_each_month(start))
        with click.progressbar(all_months, item_show_func=_show_date) as bar:
            for month in bar:
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
                        orphaners_gone(connection, month),
                        committers_gone(connection, month),
                    ]
                )


if __name__ == "__main__":
    main()
