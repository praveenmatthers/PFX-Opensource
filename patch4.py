with open('AE_RenderSlave.py', 'r') as f:
    content = f.read()

search = """    parser.add_argument("--ip",      default=None,
                        help=("Override the IP this machine advertises to the manager. "
                              "Use when auto-detection picks the wrong network adapter. "
                              "Example: --ip 192.168.1.25"))
    args = parser.parse_args()"""

replace = """    parser.add_argument("--ip",      default=None,
                        help=("Override the IP this machine advertises to the manager. "
                              "Use when auto-detection picks the wrong network adapter. "
                              "Example: --ip 192.168.1.25"))
    parser.add_argument("--secret",  default="",
                        help="Shared secret for API and TCP authentication")
    args = parser.parse_args()"""

content = content.replace(search, replace)

search2 = """    slave = SlaveState(
        manager_ip  = args.manager,
        listen_port = args.port,
        aerender    = aerender,
        ae_version  = ae_ver,
        name_override = args.name,
        ip_override   = args.ip
    )"""

replace2 = """    slave = SlaveState(
        manager_ip  = args.manager,
        listen_port = args.port,
        aerender    = aerender,
        ae_version  = ae_ver,
        name_override = args.name,
        ip_override   = args.ip,
        secret        = args.secret
    )"""

content = content.replace(search2, replace2)

with open('AE_RenderSlave.py', 'w') as f:
    f.write(content)
