from flask import Blueprint
from flask import jsonify, request
from flask import current_app
from app.dao import dao
from app.flask_helpers import build_response
import re

tags = Blueprint("tags", __name__)

@tags.route('/tags/<string:itemcode>')
def sqlite_query(itemcode):
  itemcode_reg = current_app.config["ITEMCODE_REGEX"]
  if re.match(itemcode_reg, itemcode):
    query = "SELECT * from tags where itemcode='{}';".format(itemcode)
    result = dao.execute_sqliteQuery_toJson(query)
    return build_response(result, 200)
  return jsonify(message="code \'{}\' is not well formatted".format(itemcode)), 400

@tags.route('/tags',methods=['POST'])
def fetchByItemcodes():
  if request.is_json:
    req = request.get_json()
    codes = req["itemcodes"]
    # TODO: Add watchguard against sql injections
    query="select * from tags where itemcode in ({})".format(",".join(["'{}'".format(x) for x in codes]))
    result = dao.execute_sqliteQuery_toJson(query)
    return build_response(result, 200)
    #return jsonify(message="\'{}\'".format(req))
  else:
    return "Request was not JSON", 400
