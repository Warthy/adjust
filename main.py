from collections import Counter

from Database import Database
import os
import csv
from psycopg2.extensions import AsIs

SHARD_COUNT = 3
IMPORT_DIR = 'csv/'


def main():
    # Connect to psql locally
    db = Database(
        host="localhost",
        database="adjust",
        user="postgres",
        password="postgres"
    )

    # Create 3 table shards
    for i in range(1, SHARD_COUNT+1):
        db.execute(
            """
             CREATE TABLE IF NOT EXISTS installs_by_country_{shard} (
                    "id" INT PRIMARY KEY,
                    "country" VARCHAR(255) NOT NULL ,
                    "created_at" date,
                    "paid" BOOLEAN,
                    "installs" INT
            )
            """.format(shard=i)
        )

    # Insert csv file into tables
    metrics = Counter()
    shard = 1
    for filename in os.listdir(IMPORT_DIR):
        if filename.endswith(".csv"):
            with open(IMPORT_DIR+"/"+filename) as csv_file:
                reader = csv.reader(csv_file, delimiter=',')

                values = []
                keys = reader.__next__()[1:]
                create_index = keys.index("created_at")
                country_index = keys.index("country")
                for row in reader:
                    data = row[1:]
                    values.append(AsIs(tuple(data)).__str__())

                    # Metrics is done here rather than with sql query as it avoid us to go through all data once again
                    if data[create_index].startswith("2019-05"):
                        metrics[data[country_index]] += 1

            query = "INSERT INTO installs_by_country_{shard} ({columns}) VALUES {values}".format(shard=shard, columns=",".join(keys), values=",".join(values))
            db.execute(query)
            shard += 1
        else:
            continue

    # Return metrics
    print(metrics)
    db.close()


if __name__ == "__main__":
    # execute only if run as a script
    main()
