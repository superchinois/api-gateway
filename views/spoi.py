from flask import Blueprint
from flask import jsonify, request
from app.utils.flask_helpers import build_response
from app.dao import dao

spoi = Blueprint("spoi", __name__)

@spoi.route('/spoi/clients')
def search_clients():
  search_param = request.args.get("search")
  if search_param:
    suppliers = dao.getBusinessPartners(search_param.upper())
    suppliersResult = suppliers[suppliers["cardcode"].str.startswith("C")|suppliers["cardcode"].str.startswith("c")]
    return build_response(dao.dfToJson(suppliersResult))
  return jsonify(message="search parameter is missing..."), 400

@spoi.route('/spoi/batches')
def getBatchData():
  spoi_itemcodes = [
    "321020","321024","321022","321021",
    "321013","321012","321014",
    "321034","321035",
    "321329","321060","321040"
  ]
  batchData = dao.getBatchNumbersAndQuantities(spoi_itemcodes)
  return build_response(dao.dfToJson(batchData))