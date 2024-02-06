import os
import socket
import ssl

import certifi
import duckdb
from dotenv import load_dotenv

load_dotenv()


def get_domain_name(ip: str) -> str:
    context = ssl.create_default_context(cafile=certifi.where())
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    # hostname = socket.gethostbyaddr(ip)[0]
    with socket.create_connection((ip, 443)) as sock:
        with context.wrap_socket(sock, server_hostname=ip) as sslsock:
            cert = sslsock.getpeercert()
            subject = dict(x[0] for x in cert["subject"])
            # F00 DEBUG: print(f"Certificate subject: {subject['commonName']}")
            return subject["commonName"]


def check_port(ip: str) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.25)
    try:
        s.connect((ip, 443))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()


pg_url = os.getenv("DB_URL")

duckdb.execute(
    f"""--sql
            INSTALL postgres; LOAD postgres;
            ATTACH 'postgres:{pg_url}' as postgres
    """
)

latest_row = """
SELECT MAX(last_row) from postgres.caddy_fct_metadata
"""
last_row = duckdb.execute(latest_row).fetchone()[0]

print(f"inserting new data after {last_row}")

new_data = """
SELECT
    message[:24]::timestamp as time_reported,
    json(regexp_replace(
        message[position('{' in message):],
        '("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):[\s]*\[[^]]*\]',
		'\\1:[]',
        'g')) as j

	FROM postgres.dockerlogs
	WHERE message LIKE '%%http.log.access.log0%%' 
    AND json_valid(regexp_replace(
        message[position('{' in message):],
        '("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):[\s]*\[[^]]*\]',
		'\\1:[]',
        'g'))
	AND id > """ + str(last_row)

insert = f"""
INSERT INTO postgres.caddy_fct
SELECT 
-- json -> json -> json -> varchar
j->'request'->'headers'->'Cf-Connecting-Ip'->>0 as ip,
j->'request'->>'host' as host,
j->'request'->>'uri' as uri,
time_reported
FROM ({new_data})
"""

duckdb.execute(insert)

update_metadata = """--sql
INSERT INTO postgres.caddy_fct_metadata
SELECT MAX(id::int) AS last_row,
NOW() as date
FROM postgres.dockerlogs
"""

duckdb.execute(update_metadata)

last_row = duckdb.execute("SELECT MAX(id::int) FROM postgres.dockerlogs").fetchone()[0]

print(f"new last row is : {last_row}")


missing_domain = """
SELECT DISTINCT ip 
FROM postgres.caddy_fct
WHERE ip not in (SELECT DISTINCT ip from postgres.ip2domain)
"""

insert_domain = """
INSERT INTO postgres.ip2domain VALUES 
"""
for row in duckdb.execute(missing_domain).fetchall():
    ip = row[0]
    if check_port(ip):
        try:
            domain = get_domain_name(ip)
            print(f"{ip}: {domain}")
            insert_domain += f"('{ip}', '{domain}'),"
        except:
            pass
    else:
        insert_domain += f"('{ip}', '{None}'),"

duckdb.execute(insert_domain[:-1])
