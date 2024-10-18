import json
from win10toast import ToastNotifier

def win_toast_rmq_callback(ch, method, properties, body):
  body_str = body.decode('utf-8')
  toaster = ToastNotifier()

  try:
    json_data = json.loads(body_str)
    print("Mensagem JSON:", json_data)
    toaster.show_toast(json_data["title"], json_data["message"], duration=10)
  except json.JSONDecodeError as e:
    print("Erro ao decodificar JSON:", e)