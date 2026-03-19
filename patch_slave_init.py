with open('AE_RenderSlave.py', 'r') as f:
    content = f.read()

search = """    def __init__(self, manager_ip: str, name: str, port: int):
        self.manager_ip        = manager_ip"""

replace = """    def __init__(self, manager_ip: str, name: str, port: int, secret: str = ""):
        self.manager_ip        = manager_ip
        self.secret            = secret"""

content = content.replace(search, replace)

search2 = """    slave = SlaveState(args.manager, args.name, args.port)"""

replace2 = """    slave = SlaveState(args.manager, args.name, args.port, args.secret)"""

content = content.replace(search2, replace2)

with open('AE_RenderSlave.py', 'w') as f:
    f.write(content)
