from flask import make_response
from werkzeug.datastructures import Headers
from werkzeug.wsgi import FileWrapper
from flask import Response

def build_response(jsonString, status=200):
  resp = make_response(jsonString, status)
  resp.headers["Content-Type"]='application/json'
  return resp

def send_file_response(inMemoryObject, filename):
  # Rewind the buffer.
  inMemoryObject.seek(0)
  w = FileWrapper(inMemoryObject)
  d = Headers()
  d.add('Content-Disposition', 'attachment',filename=f"{filename}")
  return Response(w, headers=d, mimetype="application/octet-stream", direct_passthrough=True)