from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MINIO_ROOT_USER: str = ""
    MINIO_ROOT_PASSWORD: str = ""
    MINIO_BUCKET: str = ""

    POSTGRES_DB: str = "ducklake"
    POSTGRES_USER: str = "ducklake"
    POSTGRES_PASSWORD: str = "ducklake"
    POSTGRES_PORT: int = 5432
    POSTGRES_HOST: str = ""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
