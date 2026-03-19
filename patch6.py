with open('AE_RenderSlave.py', 'r') as f:
    content = f.read()

search = """        try:
            msg    = json.loads(data.decode())
            action = msg.get("action", "")

            if action == "RENDER":"""

replace = """        try:
            msg    = json.loads(data.decode())
            action = msg.get("action", "")

            if slave.secret and msg.get("secret") != slave.secret:
                clog(f"Unauthorized connection attempt from {addr[0]}", "WARN")
                response = json.dumps({"status": "unauthorized", "error": "unauthorized"}).encode()
                conn.sendall(response)
                conn.close()
                return

            if action == "RENDER":"""

content = content.replace(search, replace)
with open('AE_RenderSlave.py', 'w') as f:
    f.write(content)
