# flask_web/app.py
# coding: utf-8
from flask_cors import CORS
from app.factory import create_app
from app.cache import cache
from app.dao import dao
from app.cache_dao import cache_dao
if __name__ == '__main__':
  app = create_app()
  cache.init_app(app)
  CORS(app)
  app.config.from_object('app.config.ProductionConfig')
  dao.init_app(app)
  cache_dao.init_app(app)
  app.run(debug=True, host='0.0.0.0', port=app.config["APP_RUNNING_PORT"])
