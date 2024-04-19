import logging
import os
import plistlib
import subprocess

import duckdb
import structlog
from infisical_client import ClientSettings, GetSecretOptions, InfisicalClient

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.add_log_level,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
)

log = structlog.get_logger()


# poetry run python3 -c "from screentime import create; create()"
def create():
    # get the path of the virtualenv local to this script
    # get the path of the script create a plist file from this data
    # move this plist to the ~/Library/LaunchAgents/ directory
    # load the job
    command = [
        os.environ.get("VIRTUAL_ENV") + "/bin/python",
        os.path.realpath(__file__),
    ]
    print(command)
    d = {
        "Label": "io.screen.thyme",
        "ProgramArguments": command,
        # ["/usr/bin/open", "-W", "/Applications/Calculator.app"],
        "RunAtLoad": True,
        "KeepAlive": True,
        "StartOnMount": True,
        "StartInterval": 3600,
        "ThrottleInterval": 3600,
        # https://apple.stackexchange.com/questions/435496/launchd-service-logs
        "StandardErrorPath": "/tmp/local.job.err",
        "StandardOutPath": "/tmp/local.job.out",
    }
    file_ = os.path.join(
        "/Users/edwinzamudio/Library/LaunchAgents/io.screen.thyme.plist"
    )
    print(f"Creating PLIST file {file_}")
    with open(file_, "wb+") as fp:
        plistlib.dump(d, fp)

    print("Loading job")
    command = f"launchctl unload -w {file_}"
    subprocess.run(command, shell=True)
    command = f"launchctl load -w {file_}"
    subprocess.run(command, shell=True)


def apple_exporter():
    """
    flow:
        get last row seen from postgres
        pull all data after last row from sqlite and insert into postgres
        update metadata with new last row

    Notes:
    When using postgres ext, it requires you use SQL syntax for it.
    ex. NOW() is only available in postgres and not duckdb
    """
    latest_row = "SELECT MAX(last_row) FROM postgres.apple_screentime_metadata;"
    last_row = duckdb.execute(latest_row).fetchone()[0]
    log.info(f"apple: adding rows after {last_row}")

    insert_new = f"""--sql
        INSERT INTO postgres.apple_screentime
        SELECT
        Z_PK as z_pk,
        ZSTREAMNAME as zstreamname,
        ZVALUESTRING as zvaluestring,
        to_timestamp(ZCREATIONDATE::DOUBLE + 978307200)  AS zcreationdate,
        to_timestamp(ZENDDATE::DOUBLE + 978307200) AS zenddate,
        to_timestamp(ZLOCALCREATIONDATE::DOUBLE + 978307200) AS zlocalcreationdate,
        to_timestamp(ZSTARTDATE::DOUBLE + 978307200) AS zstartdate
        FROM apple.zobject
        WHERE Z_PK::INT > {last_row}
        """

    duckdb.execute(insert_new)

    rows = duckdb.execute(
        f"SELECT COUNT(1) FROM apple.zobject WHERE Z_PK::INT > {last_row} LIMIT 1"
    ).fetchone()[0]
    log.info(f"apple: Inserted {rows} new rows!")

    update_metadata = """--sql
        INSERT INTO postgres.apple_screentime_metadata
        SELECT MAX(Z_PK::INT) AS last_row,
        NOW() as date
        FROM apple.zobject
        """
    duckdb.execute(update_metadata)

    last_row = duckdb.execute("SELECT MAX(Z_PK::INT) FROM apple.zobject").fetchone()[0]
    log.info(f"apple: Last row seen {last_row}, Metadata updated!")


def aw_exporter():
    latest_row = "SELECT MAX(last_row) FROM postgres.eventmodel_metadata;"
    last_row = duckdb.execute(latest_row).fetchone()[0]
    log.info(f"aw: adding rows after {last_row}")

    insert_new = f"""--sql
        INSERT INTO postgres.eventmodel
        SELECT *
        FROM aw.eventmodel
        WHERE id::int > {last_row}::int
        """
    duckdb.execute(insert_new)

    rows = duckdb.execute(
        f"SELECT COUNT(1) FROM aw.eventmodel WHERE id::int > {last_row}::int LIMIT 1"
    ).fetchone()[0]
    log.info(f"aw: Inserted {rows} new rows!")

    update_metadata = """--sql
        INSERT INTO postgres.eventmodel_metadata
        SELECT MAX(id::int) AS last_row,
        NOW() as date
        FROM aw.eventmodel
        """

    duckdb.execute(update_metadata)

    last_row = duckdb.execute("SELECT MAX(id::int) FROM aw.eventmodel").fetchone()[0]
    log.info(f"aw: Last row seen {last_row}, Metadata updated!")


if __name__ == "__main__":
    client = InfisicalClient(
        ClientSettings(
            access_token="st.0f1fb32c-f2ad-429f-b3b8-bff539975f5f.1910ac88e175bc3ccee1f755d14e44cd.63938e0140a19371329eff95cb2a7cb0",
            site_url="http://spark:8080",
        )
    )
    postgres_url = client.getSecret(
        options=GetSecretOptions(
            environment="prod",
            project_id="d62f85ea-2258-45ae-afa2-857ece8d8743",
            secret_name="LOGS_DB",
        )
    ).secret_value
    log.info("Running pipeline using infisical client")

    duckdb.execute(
        f"""--sql
            INSTALL postgres; LOAD postgres;
            INSTALL sqlite; LOAD sqlite;
            SET GLOBAL sqlite_all_varchar = true;
            ATTACH 'postgres:{postgres_url}' as postgres;
            ATTACH 'sqlite:/Users/edwinzamudio/Library/Application Support/Knowledge/knowledgeC.db' AS apple;
            ATTACH 'sqlite:/Users/edwinzamudio/Library/Application Support/activitywatch/aw-server/peewee-sqlite.v2.db' AS aw;
        """
    )

    apple_exporter()
    aw_exporter()
