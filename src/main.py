import duckdb

from src.settings import settings


def _install_extensions():
    duckdb.execute(
        """
        INSTALL ducklake;
        INSTALL httpfs;
        INSTALL postgres;
        LOAD ducklake;
        LOAD httpfs;
        LOAD postgres;
        """
    )


def attach_catalog():
    _install_extensions()
    duckdb.execute(f"""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID '${settings.MINIO_ROOT_USER}',
            SECRET '${settings.MINIO_ROOT_PASSWORD}',
            ENDPOINT 'minio:9000',
            URL_STYLE 'path',
            USE_SSL false
        );

        ATTACH 'ducklake:postgres:dbname=${settings.POSTGRES_DB} user=${settings.POSTGRES_USER} password=${settings.POSTGRES_PASSWORD} host={settings.POSTGRES_HOST} port={settings.POSTGRES_PORT}'
        AS my_lake (DATA_PATH 's3://${settings.MINIO_BUCKET}/');

        USE my_lake;

    """)
