#!/usr/bin/env python3

# SPDX-FileCopyrightText: Contributors to the Fedora Project
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Collect data from DataNommer database and store it in a SQLite database for
querying.

This requires access to the datanommer database, ideally on localhost.
It will read ~/.pgpass to get the password.
"""

import json
import os
import socket
import sqlite3
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2
from dateutil.parser import parse as date_parse

DN_DB = "datanommer2"
DN_USER = "datanommer_ro"
DN_HOST = "localhost"
TOPIC_ACTION = "org.fedoraproject.{env}.pagure.project.{action}"
TOPIC_COMMIT = "org.fedoraproject.{env}.git.receive"


def get_env():
    return "stg" if ".stg." in socket.gethostname() else "prod"


def read_pgpass():
    credentials = defaultdict(dict)
    with open(os.path.expanduser("~/.pgpass")) as fh:
        for line in fh:
            hostname, port, database, username, password = line.strip().split(":")
            credentials[database][username] = password
    return credentials


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
    for col in ("timestamp", "year", "month", "by"):
        connection.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_{col} ON {table}({col})")


def next_month(date):
    newdate = date + timedelta(days=32)
    newdate.replace(day=1)
    return newdate


def message_from_row(row):
    return {
        "id": row[0],
        "topic": row[1],
        "timestamp": row[2],
        "body": json.loads(row[3]),
        "headers": row[4] or {},
    }


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


def record_pagure_messages(topic, table, dndb, statsdb, start, end=None):
    make_db(statsdb, table)
    query = (
        "SELECT msg_id, topic, timestamp, msg, headers FROM messages "
        "WHERE topic = %s AND timestamp >= %s"
    )
    params = [topic, start.isoformat()]
    if end:
        query += " AND timestamp < %s"
        params.append(end.isoformat())
    dndb.execute(query, params)
    for row in dndb:
        insert_message(table, statsdb, message_from_row(row))
        statsdb.commit()


def insert_message(table, db, message):
    try:
        msg_date = date_parse(message["headers"]["sent-at"])
    except KeyError:
        msg_date = message["timestamp"]
    try:
        package = message["body"]["project"]["fullname"]
    except KeyError:
        try:
            package = message["body"]["commit"]["repo"]
            if "namespace" in message["body"]["commit"]:
                package = f"{message['body']['commit']['namespace']}/{package}"
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


def record_action(action, dndb, statsdb, start, end=None):
    return record_pagure_messages(
        topic=TOPIC_ACTION.format(env=get_env(), action=action),
        table=f"{action}ed",
        dndb=dndb,
        statsdb=statsdb,
        start=start,
        end=end,
    )


def record_commits(dndb, statsdb, start, end=None):
    make_db(statsdb, "commits")
    make_db(statsdb, "retired")
    query = (
        "SELECT msg_id, topic, timestamp, msg, headers FROM messages "
        "WHERE topic = %s AND timestamp >= %s"
    )
    params = [TOPIC_COMMIT.format(env=get_env()), start.isoformat()]
    if end:
        query += " AND timestamp < %s"
        params.append(end.isoformat())
    dndb.execute(query, params)
    for row in dndb:
        message = message_from_row(row)
        insert_message("commits", statsdb, message)
        if is_retirement(message):
            insert_message("retired", statsdb, message)
        statsdb.commit()


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-s", "--since", required=True)
    parser.add_argument("output")
    return parser.parse_args()


def main():
    args = parse_args()
    start = date_parse(args.since)
    statsdb = sqlite3.connect(args.output)
    pg_credentials = read_pgpass()
    dndb_conn = psycopg2.connect(
        dbname=DN_DB,
        user=DN_USER,
        password=pg_credentials[DN_DB][DN_USER],
        host=DN_HOST,
    )
    dndb = dndb_conn.cursor()
    while start <= datetime.now():
        print(f"\r{start.year}-{start.month:02}.", end="", flush=True)
        end = next_month(start)
        record_action("orphan", dndb, statsdb, start, end)
        print(".", end="", flush=True)
        record_action("adopt", dndb, statsdb, start, end)
        print(".", end="", flush=True)
        record_commits(dndb, statsdb, start, end)
        print(".", end="", flush=True)
        start = next_month(start)
    print("\rdone.")


if __name__ == "__main__":
    main()
