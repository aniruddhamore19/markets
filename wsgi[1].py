"""Entry point for web hosts (Render, Railway, etc.).

The host runs:  gunicorn wsgi:app --workers 1 --threads 8
It must stay at ONE worker: the latest market data is kept in that worker's memory,
and a second worker would fetch everything twice and show different numbers.
"""
from mini_bloomberg_web import app, start_background_refresh

start_background_refresh()
