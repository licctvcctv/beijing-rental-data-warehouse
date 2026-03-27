#!/bin/bash
set -e

echo ">>> Superset bootstrap: upgrading DB ..."
superset db upgrade

echo ">>> Creating admin user ..."
superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --firstname "${ADMIN_FIRSTNAME:-Rental}" \
  --lastname "${ADMIN_LASTNAME:-Admin}" \
  --email "${ADMIN_EMAIL:-admin@rental.local}" \
  --password "${ADMIN_PASSWORD:-admin}" \
  || true

echo ">>> Initializing roles & permissions ..."
superset init

echo ">>> Starting Superset server ..."
exec gunicorn \
  --bind "0.0.0.0:8088" \
  --workers 2 \
  --timeout 120 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  "superset.app:create_app()"
