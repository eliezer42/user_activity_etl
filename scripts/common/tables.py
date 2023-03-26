import common.base as base
from sqlalchemy import Integer, String, Float, Boolean, Date, DateTime
from sqlalchemy.orm import mapped_column

class User(base.Base):
    __tablename__ = "users"

    id = mapped_column(Integer, primary_key=True)
    first_name = mapped_column(String(80))
    last_name = mapped_column(String(80))
    email = mapped_column(String(320))
    gender = mapped_column(String(16))
    ip_address = mapped_column(String(40))  # It takes into account IPv4 and IPv6 address length
    created_at = mapped_column(Date)
    updated_at = mapped_column(Date)
    password = mapped_column(String(64))    # Suitable for MD5 and SHA-256 hash length
    status = mapped_column(Boolean)
    latitude = mapped_column(Float)
    longitude = mapped_column(Float)
    country = mapped_column(String(64))
    region = mapped_column(String(64))
    city = mapped_column(String(64))
    timezone =mapped_column(String(32))
    isp_name = mapped_column(String(128))
    updated = mapped_column(Boolean)        # If set to True, the record was updated
    migrated_at = mapped_column(DateTime)