function escStr(s) {
    return String(s)
        .replace(/\\/g, "\\\\")
        .replace(/"/g, '\\"')
        .replace(/\n/g, "\\n")
        .replace(/\r/g, "\\r")
        .replace(/\t/g, "\\t");
}

function toJSON(val) {
    if (val === null || val === undefined) return "null";
    if (typeof val === "string") return '"' + escStr(val) + '"';
    if (typeof val === "number" || typeof val === "boolean") return String(val);
    if (val instanceof Array) {
        var arr = [];
        for (var i = 0; i < val.length; i++) arr.push(toJSON(val[i]));
        return "[" + arr.join(",") + "]";
    }
    if (typeof val === "object") {
        var obj = [];
        for (var k in val) {
            if (val.hasOwnProperty(k)) {
                obj.push('"' + escStr(k) + '":' + toJSON(val[k]));
            }
        }
        return "{" + obj.join(",") + "}";
    }
    return '""';
}

var payloadObj = {
    submitted_at: "2024-01-01 12:00:00",
    machine: "host",
    user: "user",
    project: "C:\\path\\to\\project.aep",
    priority: 5,
    chunk_size: 10,
    required_effects: [{matchName:"efx1", displayName:"Efx 1"}],
    jobs: [
        {
            comp_name: "Comp 1",
            project_path: "C:\\path\\to\\project.aep",
            output_path: "\\\\server\\path",
            output_path_orig: "C:\\path",
            start_frame: 0,
            end_frame: 100,
            fps: 24,
            width: 1920,
            height: 1080,
            duration_frames: 101,
            rq_index: 1,
            hostname: "host",
            is_video: false
        }
    ]
};

console.log(toJSON(payloadObj));
