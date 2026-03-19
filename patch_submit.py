with open('AE_Submit.jsx', 'r') as f:
    content = f.read()

if "var MANAGER_SECRET =" not in content:
    search = """    var DEFAULT_HOST = "127.0.0.1";
    var DEFAULT_PORT = 9876;
    var WATCH_DIR = Folder.temp.fsName;
    var SEP = ($.os.indexOf("Windows") !== -1) ? "\\\\" : "/";"""

    replace = """    var DEFAULT_HOST = "127.0.0.1";
    var DEFAULT_PORT = 9876;
    var MANAGER_SECRET = "";
    var WATCH_DIR = Folder.temp.fsName;
    var SEP = ($.os.indexOf("Windows") !== -1) ? "\\\\" : "/";"""

    content = content.replace(search, replace)

if "escStr(MANAGER_SECRET)" not in content:
    search2 = """        var payload = '{'
            + '"submitted_at":"' + escStr(ts) + '",'"""

    replace2 = """        var payload = '{'
            + '"secret":"' + escStr(MANAGER_SECRET) + '",'
            + '"submitted_at":"' + escStr(ts) + '",\'"""

    content = content.replace(search2, replace2)

with open('AE_Submit.jsx', 'w') as f:
    f.write(content)
