from flask import Flask
import sys

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return "Hello World Service!"

@app.route('/__health_check__.html', methods=['GET'])
def health_check():
    return 'OK'

def __service_main__(port):
    app.run('0.0.0.0', port=int(port), debug=False)

if __name__ == '__main__':
    __service_main__(1024)
