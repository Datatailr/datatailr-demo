import dt.service
import requests

SERVICE_NAME = 'Test flask service'

host, port = dt.service.service_host_and_port(SERVICE_NAME)

url = f'http://{host}:{port}'
health_check_url = url + '/__health_check__.html'

response = requests.get(url, timeout=10)

print(response.text)
assert requests.get(health_check_url, timeout=10).text == 'OK'
