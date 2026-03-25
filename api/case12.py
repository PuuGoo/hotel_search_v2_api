import os
import tempfile
from flask import Flask, jsonify, make_response, request, send_file

from verify_hotels import verify_hotels_file_case12_chain_vho_no_chrome


app = Flask(__name__)


def _add_cors(resp):
    allow_origin = os.getenv("CORS_ALLOW_ORIGIN", "*")
    resp.headers["Access-Control-Allow-Origin"] = allow_origin
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return resp


@app.after_request
def after_request(response):
    return _add_cors(response)


@app.route("/api/case12", methods=["GET"])
def health_check():
    return jsonify({"ok": True, "service": "case12-no-chrome"})


@app.route("/api/case12", methods=["OPTIONS"])
def options_case12():
    return _add_cors(make_response("", 204))


@app.route("/api/case12", methods=["POST"])
def verify_case12():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Thiếu file upload (field name: file)"}), 400

    uploaded = request.files["file"]
    if not uploaded or not uploaded.filename:
        return jsonify({"ok": False, "error": "File upload không hợp lệ"}), 400

    filename = uploaded.filename.lower()
    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        return jsonify({"ok": False, "error": "Chỉ hỗ trợ file Excel .xlsx/.xls"}), 400

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.xlsx")
            output_path = os.path.join(temp_dir, "output.xlsx")
            uploaded.save(input_path)

            verify_hotels_file_case12_chain_vho_no_chrome(
                input_path=input_path,
                output_path=output_path,
            )

            download_name = "verified_case12.xlsx"
            return send_file(
                output_path,
                as_attachment=True,
                download_name=download_name,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex)}), 500
