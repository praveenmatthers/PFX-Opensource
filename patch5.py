with open('AE_RenderSlave.py', 'r') as f:
    content = f.read()

search = """class SlaveState:
    def __init__(self, manager_ip: str, listen_port: int,
                 aerender: str, ae_version: str,
                 name_override: str = None, ip_override: str = None):
        self._lock = threading.Lock()"""

replace = """class SlaveState:
    def __init__(self, manager_ip: str, listen_port: int,
                 aerender: str, ae_version: str,
                 name_override: str = None, ip_override: str = None,
                 secret: str = ""):
        self._lock = threading.Lock()
        self.secret             = secret"""

content = content.replace(search, replace)

search2 = """    def _build_payload(self, extra: dict = None) -> dict:
        with self._lock:
            p = dict(
                type          = "SLAVE_STATUS",
                hostname      = self.hostname,"""

replace2 = """    def _build_payload(self, extra: dict = None) -> dict:
        with self._lock:
            p = dict(
                secret        = self.secret,
                type          = "SLAVE_STATUS",
                hostname      = self.hostname,"""

content = content.replace(search2, replace2)

with open('AE_RenderSlave.py', 'w') as f:
    f.write(content)
