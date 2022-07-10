

from flask import Flask,request, jsonify
import json
import pandas as pd
from flask_cors import CORS, cross_origin
from create_table import fetch_data,upload_data,drop_table


#ROUTES
app = Flask('myapp')
app = Flask(__name__.split('.')[0])
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/commune',methods=['POST'])
@cross_origin()
def get_data():
    req_data = request.get_json()
    df = fetch_data(req_data["commune"])
    dicto = df.to_dict(orient="records")
    #df_filter =search_commune(req_data["commune"])
    #dicto=df_filter.loc[:,["Nom communes équipées","Station en fonctionnement","Nombre total vélos disponibles","Identifiant station"]].to_dict(orient="records")
    json_string = json.dumps(dicto,ensure_ascii=False)
    return json_string

@app.route('/upload',methods=['GET'])
@cross_origin()
def upload():
    upload_data()
    return {}

@app.route('/clear',methods=['GET'])
@cross_origin()
def clear():
    drop_table()
    return {}
    


# Running app
if __name__ == '__main__':
    app.run(debug=True)