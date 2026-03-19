with open('AE_RenderSlave.py', 'r') as f:
    content = f.read()

search = """    slave = SlaveState(
        manager_ip = args.manager,
        name       = args.name,
        port       = args.port,
    )"""

replace = """    slave = SlaveState(
        manager_ip = args.manager,
        name       = args.name,
        port       = args.port,
        secret     = args.secret,
    )"""

content = content.replace(search, replace)

with open('AE_RenderSlave.py', 'w') as f:
    f.write(content)
