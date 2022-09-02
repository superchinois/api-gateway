from flask import Flask
import os

PKG_NAME = os.path.dirname(os.path.realpath(__file__)).split("/")[-1]

def create_app(app_name=PKG_NAME, **kwargs):
  app = Flask(app_name)
  from views.all import bp
  from views.tags import tags
  from views.suppliers import suppliers
  from views.items import items
  from views.arrival import arrival
  from views.leaderboards import leaderboards
  # Add routes to be registered in Flask
  routes=[bp, tags, suppliers, items, arrival, leaderboards]
  for route in routes :
    app.register_blueprint(route)
  return app
