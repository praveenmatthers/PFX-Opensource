with open('AE_RenderManager.py', 'r') as f:
    content = f.read()

if "MANAGER_SECRET" not in content[:1000]:
    search = """MANAGER_PORT  = 9876        # Port the manager listens on (HTTP + slave TCP)
SLAVE_PORT    = 9877        # Port each slave listens on for job dispatch"""
    replace = """MANAGER_PORT  = 9876        # Port the manager listens on (HTTP + slave TCP)
SLAVE_PORT    = 9877        # Port each slave listens on for job dispatch
MANAGER_SECRET = ""         # Shared secret for API and TCP authentication"""
    content = content.replace(search, replace)

if "MANAGER_SECRET and data.get" not in content:
    search2 = """    def do_POST(self):
        try:
            data       = self._read_json()
            client_ip  = self.client_address[0]"""
    replace2 = """    def do_POST(self):
        try:
            data       = self._read_json()
            client_ip  = self.client_address[0]

            if MANAGER_SECRET and data.get("secret") != MANAGER_SECRET:
                self.send_json({"error": "unauthorized"}, 401)
                return"""
    content = content.replace(search2, replace2)

with open('AE_RenderManager.py', 'w') as f:
    f.write(content)
