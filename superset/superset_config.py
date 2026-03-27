import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "rental-warehouse-secret-key-change-in-prod")

SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

LANGUAGES = {
    "zh": {"flag": "cn", "name": "Chinese"},
    "en": {"flag": "us", "name": "English"},
}
BABEL_DEFAULT_LOCALE = "zh"

WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = [
    "superset.views.core.log",
    "superset.charts.api",
]

TALISMAN_ENABLED = False
SESSION_COOKIE_SAMESITE = "Lax"

class CeleryConfig:
    broker_url = os.environ.get("REDIS_URL", "redis://superset-redis:6379/0")
    result_backend = os.environ.get("REDIS_URL", "redis://superset-redis:6379/0")

CELERY_CONFIG = CeleryConfig

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://superset-redis:6379/1"),
}
