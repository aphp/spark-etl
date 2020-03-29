#!/bin/bash -e

su -c "bash /app/scripts/make-schema.sh" - postgres
node server.js