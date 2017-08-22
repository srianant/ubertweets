from flask import Flask
from flask import render_template

app = Flask(__name__)

@app.route('/')
def homepage():
    return render_template('index.html') 

@app.route('/DAG')
def DAG():
    return render_template('DAG.html') 

@app.route('/uberCluster')
def uberCluster():
    return render_template('uberCluster.html') 
