"""Pydantic-settings configuration for BerkeleyAlarms."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MQTT
    mqtt_broker: str = Field("localhost", alias="MQTT_BROKER")
    mqtt_port: int = Field(1883, alias="MQTT_PORT")
    mqtt_client_id: str = Field("berkeley-alarms", alias="MQTT_CLIENT_ID")

    # API server
    alarm_api_host: str = Field("0.0.0.0", alias="ALARM_API_HOST")
    alarm_api_port: int = Field(8084, alias="ALARM_API_PORT")

    # Config file
    alarms_config_path: Path = Field(Path("./alarms.yml"), alias="ALARMS_CONFIG_PATH")

    # Storage
    alarm_db_path: Path = Field(Path("/var/lib/berkeley/alarms.db"), alias="ALARM_DB_PATH")

    # Logging
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    model_config = {"env_file": ".env", "populate_by_name": True}


settings = Settings()
