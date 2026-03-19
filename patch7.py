with open('AE_RenderManager.py', 'r') as f:
    content = f.read()

search = """    if not required: return {}
    payload = json.dumps({"action": "PREFLIGHT", "required": required}).encode()"""

replace = """    if not required: return {}
    payload = json.dumps({"secret": MANAGER_SECRET, "action": "PREFLIGHT", "required": required}).encode()"""

content = content.replace(search, replace)

search2 = """    payload = json.dumps(dict(
        action="RENDER", job_id=job.id, comp_name=job.comp_name,
        project_path=job.project_path, output_path=job.output_path,
        start_frame=sf, end_frame=ef, rq_index=job.rq_index,
    )).encode()"""

replace2 = """    payload = json.dumps(dict(
        secret=MANAGER_SECRET, action="RENDER", job_id=job.id, comp_name=job.comp_name,
        project_path=job.project_path, output_path=job.output_path,
        start_frame=sf, end_frame=ef, rq_index=job.rq_index,
    )).encode()"""

content = content.replace(search2, replace2)

search3 = """            s.settimeout(4); s.connect((target, port))
            s.sendall(json.dumps({"action": "STOP"}).encode()); s.close()"""

replace3 = """            s.settimeout(4); s.connect((target, port))
            s.sendall(json.dumps({"secret": MANAGER_SECRET, "action": "STOP"}).encode()); s.close()"""

content = content.replace(search3, replace3)

with open('AE_RenderManager.py', 'w') as f:
    f.write(content)
