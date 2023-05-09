from flask import Flask
from database import get_5_random_customers, get_store_info, get_max_price, add_store_to_table

app = Flask(__name__)

@app.route('/')
def index():
    return "<p>Hello, welcome to this train app</p>"

@app.route('/customers/show')
def show_customers():
    return get_5_random_customers()

@app.route('/stores/<store_id>')
def store_info(store_id):
    return get_store_info(store_id)

@app.route('/prices/max')
def gmp():
    return get_max_price()

@app.route('/stores/add')
def add_store():
    return add_store_to_table()

if __name__ == "__main__":
    app.run(port=8080)