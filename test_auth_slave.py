import subprocess
import time
import json
import socket
import os

os.environ['QT_QPA_PLATFORM'] = 'offscreen'

print("Starting Manager...")
mgr = subprocess.Popen(["python3", "AE_RenderManager.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)

print("Starting Slave...")
slave = subprocess.Popen(["python3", "AE_RenderSlave.py", "--secret", "mysecret", "--manager", "127.0.0.1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(2)

def tcp_send(payload):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("127.0.0.1", 9877))
        s.sendall(json.dumps(payload).encode())
        data = s.recv(1024)
        s.close()
        return json.loads(data.decode()) if data else None
    except Exception as e:
        return f"Error: {e}"

print("Test TCP RENDER with correct secret:")
res = tcp_send({"action": "RENDER", "secret": "mysecret"})
print(res)

print("Test TCP RENDER with wrong secret:")
res = tcp_send({"action": "RENDER", "secret": "wrong"})
print(res)

slave.terminate()
mgr.terminate()

out, err = slave.communicate()
print("Slave output:", out.decode())
print("Slave err:", err.decode())
