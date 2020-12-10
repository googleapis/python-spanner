# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import argparse

from google.cloud.spanner_dbapi import connect


def manual_commit(instance_id, database_id):
    """Use connection in !autocommit mode."""
    # [START spanner_manual_commit]

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    cursor.execute(
        """CREATE TABLE Singers (
                SingerId     INT64 NOT NULL,
                FirstName    STRING(1024),
                LastName     STRING(1024),
                SingerInfo   BYTES(MAX)
            ) PRIMARY KEY (SingerId)"""
    )
    print("Transaction begun.")

    cursor.execute(
        """INSERT INTO Singers (SingerId, FirstName, LastName) VALUES
            (12, 'Melissa', 'Garcia'),
            (13, 'Russell', 'Morales'),
            (14, 'Jacqueline', 'Long'),
            (15, 'Dylan', 'Shaw')"""
    )

    cursor.execute("""SELECT * FROM Singers WHERE SingerId = 13""")
    connection.commit()
    print("Transaction committed.")

    print("SingerId: {}, AlbumId: {}, AlbumTitle: {}".format(*cursor.fetchone()))

    connection.close()
    # [END spanner_manual_commit]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database-id", help="Your Cloud Spanner database ID.", default="example_db",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("manual_commit", help=manual_commit.__doc__)
    args = parser.parse_args()
    if args.command == "manual_commit":
        manual_commit(args.instance_id, args.database_id)
    else:
        print(f"Command {args.command} did not match expected commands.")
