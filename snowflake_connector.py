import asyncio
from typing import Union

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.constants import QueryStatus


def decrypt_private_key(private_key: str, passphrase: Union[str, None]) -> bytes:
    """Decrypts a PEM private key."""
    p_key = serialization.load_pem_private_key(
        private_key.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class SnowflakeConnector:
    def __init__(
        self,
        account_name: str,
        username: str,
        private_key: str,
        private_key_passphrase: Union[str, None],
    ):
        self.account_name = account_name
        self.username = username
        self.private_key = private_key
        self.private_key_passphrase = private_key_passphrase

    @property
    def decrypted_private_key(self) -> bytes:
        return decrypt_private_key(self.private_key, self.private_key_passphrase)

    def __enter__(self):
        self.con = snowflake.connector.connect(
            user=self.username,
            private_key=self.decrypted_private_key,
            account=self.account_name,
        )

        return self

    def __exit__(self, *exc):
        self.con.close()

    def set_db_warehouse(self, warehouse: str):
        results = self.query(f"USE WAREHOUSE {warehouse}")
        return asyncio.run(results.fetch_results())

    def set_user_role(self, role: str):
        results = self.query(f"USE ROLE {role}")
        return asyncio.run(results.fetch_results())

    def query(self, query_str: str):
        cursor = self.con.cursor()
        cursor.execute_async(query_str)

        return QueryResult(self.con, cursor.sfqid)


class QueryResult:
    def __init__(self, con: SnowflakeConnector, query_id: str):
        self.query_id = query_id
        self.con = con
        self.cursor = con.cursor()

    def is_query_done_successfully(self):
        return self.con.get_query_status(self.query_id) == QueryStatus.SUCCESS

    def is_query_running(self):
        return (
            self.con.get_query_status(self.query_id) == QueryStatus.RUNNING
            or self.con.get_query_status(self.query_id) == QueryStatus.NO_DATA
        )

    def _fetch_results(self):
        """
        Raises ProgrammingError in case of SQL error,
        by get_results_from_sfqid
        """
        self.cursor.get_results_from_sfqid(self.query_id)

        row = self.cursor.fetchone()
        while row is not None:
            yield row
            row = self.cursor.fetchone()

    async def fetch_results(self):
        while self.is_query_running():
            await asyncio.sleep(0.1)

        return self._fetch_results()

    def fetch_results_sync(self):
        self.cursor.get_results_from_sfqid(self.query_id)
        return self.cursor.fetchall()
