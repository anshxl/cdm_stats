"""WSGI entry point for gunicorn (used by Railway)."""

from cdm_stats.dashboard.app import app, register_all_callbacks

register_all_callbacks()
server = app.server
