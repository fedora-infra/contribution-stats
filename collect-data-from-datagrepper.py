#!/usr/bin/env python3

# SPDX-FileCopyrightText: Contributors to the Fedora Project
# SPDX-License-Identifier: GPL-3.0-or-later

import sqlite3
from datetime import datetime, timedelta

import click
import requests

SERVERS = {
    "prod": {
        "url": "https://apps.fedoraproject.org/datagrepper",
    },
    "stg": {
        "url": "https://apps.stg.fedoraproject.org/datagrepper",
    },
}
TOPIC_ACTION = "org.fedoraproject.{env}.pagure.project.{action}"
TOPIC_COMMIT = "org.fedoraproject.{env}.git.receive"
ENV = None


def get_all_pages(params=None):
    url = f"{SERVERS[ENV]['url']}/v2/search"
    http = requests.Session()
    params = params or {}
    params["rows_per_page"] = 100
    params["page"] = 1
    total_pages = 1
    with click.progressbar(
        length=total_pages, item_show_func=lambda p: f"Page {p or 1}/{total_pages}"
    ) as bar:
        while params["page"] <= total_pages:
            response = http.get(url, params=params)
            response.raise_for_status()
            response = response.json()
            total_pages = response["pages"]
            bar.length = total_pages
            yield response["raw_messages"]
            bar.update(1, params["page"])
            params["page"] += 1


def make_db(connection, table):
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            msgid VARCHAR(254) NOT NULL,
            timestamp DATETIME NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            by VARCHAR(254) NOT NULL,
            package VARCHAR(254) NOT NULL,
            PRIMARY KEY (msgid)
        )
    """
    )


def next_month(date):
    newdate = date + timedelta(days=32)
    newdate.replace(day=1)
    return newdate


def is_retirement(message):
    try:
        dead_package = message["body"]["commit"]["stats"]["files"]["dead.package"]
    except KeyError:
        return False
    if dead_package["additions"] == 0 and dead_package["deletions"] != 0:
        # dead.package file was not added, bailing
        return False
    if message["body"]["commit"]["branch"] not in ("main", "rawhide"):
        return False
    return True


def record_pagure_messages(topic, table, db, start, end=None):
    make_db(db, table)
    params = {"start": start.isoformat(), "topic": topic}
    if end:
        params["end"] = end.isoformat()
    for page in get_all_pages(params):
        for message in page:
            insert_message(table, db, message)
        db.commit()


def insert_message(table, db, message):
    msg_date = datetime.fromisoformat(message["headers"]["sent-at"])
    try:
        package = message["body"]["project"]["fullname"]
    except KeyError:
        try:
            package = (
                f"{message['body']['commit']['namespace']}/{message['body']['commit']['repo']}"
            )
        except KeyError:
            print(message["body"])
            raise
    try:
        agent = message["body"]["agent"]
    except KeyError:
        try:
            agent = message["body"]["commit"]["agent"]
        except KeyError:
            print(message["body"])
            raise
    try:
        db.execute(
            f"INSERT INTO {table} (msgid, timestamp, year, month, by, package)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                message["id"],
                msg_date.isoformat(),
                msg_date.year,
                msg_date.month,
                agent,
                package,
            ),
        )
    except sqlite3.IntegrityError:
        # Same message id, ignore
        return
    db.commit()


def record_action(action, connection, start):
    return record_pagure_messages(
        topic=TOPIC_ACTION.format(env=ENV, action=action),
        table=f"{action}ed",
        db=connection,
        start=start,
    )


def record_commits(db, start, end):
    make_db(db, "commits")
    make_db(db, "retired")
    params = {"start": start.isoformat(), "topic": TOPIC_COMMIT.format(env=ENV)}
    if end:
        params["end"] = end.isoformat()
    for page in get_all_pages(params):
        for message in page:
            insert_message("commits", db, message)
            if is_retirement(message):
                insert_message("retired", db, message)
        db.commit()


@click.command()
# @click.option("-s", "--since")
# @click.option("-p", "--split-pages", is_flag=True)
@click.option("-e", "--env", type=click.Choice(SERVERS.keys()), required=True)
@click.option("-o", "--output", type=click.Path(), required=True)
def main(env, output):
    global ENV
    ENV = env
    # start = datetime(2020, 8, 1)
    start = datetime(2023, 8, 1)
    connection = sqlite3.connect(output)
    # for action in ("orphan", "adopt"):
    #     record_action(action, connection, start)
    while start <= datetime.now():
        end = next_month(start)
        record_commits(connection, start, end)
        start = next_month(start)


if __name__ == "__main__":
    main()
