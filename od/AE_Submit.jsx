// AE_Submit_v6.jsx  —  AEREN Farm Submission
// 2026 - All rights reserved - Praveen Brijwal
//
// Run: File > Scripts > Run Script File inside After Effects

var FARM_ROOT = "\\\\DESKTOP-3BK9PQH\\Projects\\AE_RenderManager\\AEREN_DATA_LOGS";
var SUBMIT_VER = "6.0.2";

// =============================================================================
// PATH UTILITIES
// =============================================================================

function normPath(p) {
    p = String(p).replace(/\//g, "\\");
    while (p.length > 3 && p.charAt(p.length - 1) === "\\")
        p = p.slice(0, p.length - 1);
    return p;
}

function pjoin(a, b) {
    var base = normPath(a);
    var sub = String(b).replace(/^[\\\/]+/, "").replace(/[\\\/]+$/, "").replace(/\//g, "\\");
    if (!sub) return base;
    return base + "\\" + sub;
}

function isUNC(p) {
    return (String(p).charAt(0) === "\\" && String(p).charAt(1) === "\\");
}

function makeDirs(rawPath) {
    var p = normPath(rawPath);
    if (!p) return;
    if (isUNC(p)) {
        var inner = p.slice(2);
        var parts = inner.split("\\");
        if (parts.length < 2) return;
        var cur = "\\\\" + parts[0] + "\\" + parts[1];
        for (var i = 2; i < parts.length; i++) {
            if (!parts[i]) continue;
            cur = cur + "\\" + parts[i];
            try { var d = new Folder(cur); if (!d.exists) d.create(); } catch (e) { }
        }
    } else {
        var parts2 = p.split("\\");
        var cur2 = parts2[0];
        for (var j = 1; j < parts2.length; j++) {
            if (!parts2[j]) continue;
            cur2 = cur2 + "\\" + parts2[j];
            try { var d2 = new Folder(cur2); if (!d2.exists) d2.create(); } catch (e) { }
        }
    }
}

function parentDir(fp) {
    var p = normPath(fp);
    var i = p.lastIndexOf("\\");
    if (i <= 1 && isUNC(p)) return p;
    if (i > 0) return p.slice(0, i);
    return p;
}

function getExt(path) {
    var clean = String(path).replace(/\[#+\]/g, "").replace(/_+#+$/g, "").replace(/#+#+$/g, "");
    var parts = clean.split(".");
    if (parts.length < 2) return "exr";
    return parts[parts.length - 1].toLowerCase().replace(/[^a-z0-9]/g, "");
}

function detectType(path) {
    var ext = getExt(path);
    var video = { "mov": 1, "mp4": 1, "avi": 1, "mxf": 1, "r3d": 1, "wmv": 1, "mkv": 1 };
    return video[ext] ? "VIDEO" : "SEQUENCE";
}

function toUNC(path, hostname) {
    var p = normPath(path);
    if (isUNC(p)) return p;
    if (/^[A-Za-z]:/.test(p))
        return "\\\\" + hostname + "\\" + p.charAt(0).toUpperCase() + "$" + p.slice(2);
    return p;
}

// =============================================================================
// MISC HELPERS
// =============================================================================

function pad2(n) { return n < 10 ? "0" + n : String(n); }

function makeJobId() {
    var c = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var s = "";
    for (var i = 0; i < 8; i++) s += c.charAt(Math.floor(Math.random() * c.length));
    return s;
}

function nowStr() {
    var d = new Date();
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate())
        + " " + pad2(d.getHours()) + ":" + pad2(d.getMinutes()) + ":" + pad2(d.getSeconds());
}

function epochNow() { return Math.floor(new Date().getTime() / 1000); }

function getDateStr() {
    var d = new Date();
    return String(d.getFullYear()) + pad2(d.getMonth() + 1) + pad2(d.getDate());
}

function safeName(name) {
    return String(name).replace(/[^a-zA-Z0-9_\-\.]/g, "_").replace(/_+/g, "_")
        .replace(/^_+|_+$/g, "") || "Comp";
}

// =============================================================================
// PLUGINS & REMAP
// =============================================================================

function getUsedPlugins(comp) {
    var seen = {};
    var list = [];
    try {
        for (var li = 1; li <= comp.numLayers; li++) {
            try {
                var layer = comp.layer(li);
                if (!layer.Effects) continue;
                var efx = layer.Effects;
                for (var ei = 1; ei <= efx.numProperties; ei++) {
                    try {
                        var eff = efx.property(ei);
                        var mn = eff.matchName || "";
                        var dn = eff.name || mn;
                        if (mn && !seen[mn]) {
                            seen[mn] = true;
                            list.push({ matchName: mn, displayName: dn });
                        }
                    } catch (e2) { }
                }
            } catch (e1) { }
        }
    } catch (e0) { }
    return list;
}

function remapToUNC(proj, hostname) {
    var remaps = [];
    for (var i = 1; i <= proj.numItems; i++) {
        try {
            var item = proj.item(i);
            if (!(item instanceof FootageItem)) continue;
            if (!(item.mainSource instanceof FileSource)) continue;
            var srcFile = item.mainSource.file;
            if (!srcFile) continue;
            var orig = srcFile.fsName;
            if (!orig || !(/^[A-Za-z]:/.test(orig))) continue;
            var unc = toUNC(orig, hostname);
            remaps.push({ id: i, orig: orig, unc: unc });
            try { item.replace(new File(unc)); } catch (e) { }
        } catch (e) { }
    }
    return remaps;
}

function restoreLocalPaths(proj, remaps) {
    for (var i = 0; i < remaps.length; i++) {
        try {
            var item = proj.item(remaps[i].id);
            if (item) item.replace(new File(remaps[i].orig));
        } catch (e) { }
    }
}

// =============================================================================
// JSON
// =============================================================================

function jEsc(s) {
    return String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"')
        .replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/\t/g, "\\t");
}

function jVal(v, d) {
    d = d || 0;
    var pad = ""; for (var a = 0; a < d; a++) pad += "  ";
    var p2 = ""; for (var b = 0; b < d + 1; b++) p2 += "  ";
    if (v === null || v === undefined) return "null";
    var t = typeof v;
    if (t === "boolean") return v ? "true" : "false";
    if (t === "number") return isFinite(v) ? String(v) : "null";
    if (t === "string") return '"' + jEsc(v) + '"';
    if (v instanceof Array) {
        if (!v.length) return "[]";
        var items = [];
        for (var i = 0; i < v.length; i++) items.push(p2 + jVal(v[i], d + 1));
        return "[\n" + items.join(",\n") + "\n" + pad + "]";
    }
    var pairs = [];
    for (var k in v) {
        if (v.hasOwnProperty(k))
            pairs.push(p2 + '"' + jEsc(k) + '": ' + jVal(v[k], d + 1));
    }
    if (!pairs.length) return "{}";
    return "{\n" + pairs.join(",\n") + "\n" + pad + "}";
}

function writeJSON(filePath, obj) {
    try {
        var f = new File(filePath);
        f.encoding = "UTF-8";
        if (!f.open("w")) return false;
        f.write(jVal(obj, 0));
        f.close();
        return true;
    } catch (e) { return false; }
}

// =============================================================================
// DIALOG
// =============================================================================

function showDialog(items) {
    var dlg = new Window("dialog", "AEREN  \u2014  Submit to Farm  v" + SUBMIT_VER);
    dlg.orientation = "column";
    dlg.preferredSize.width = 460;

    var hdr = dlg.add("statictext", undefined, "AEREN Render Farm  \u2014  Submit");
    hdr.graphics.font = ScriptUI.newFont("Arial", "BOLD", 13);
    dlg.add("panel", undefined, "");

    var grp = dlg.add("group"); grp.orientation = "column"; grp.alignChildren = "left";
    grp.add("statictext", undefined, "Items to submit (" + items.length + "):");
    var lb = grp.add("listbox", [0, 0, 420, 90], []);
    lb.preferredSize = [420, 90];

    for (var i = 0; i < items.length; i++) {
        var it = items[i].item;
        var sf = 0; var ef = 0;
        try {
            sf = Math.round(it.timeSpanStart / it.comp.frameDuration);
            ef = Math.round(sf + it.timeSpanDuration / it.comp.frameDuration) - 1;
        } catch (e) { }
        var compName = it.comp ? it.comp.name : "(unknown)";
        var frameStr = sf + "\u2013" + ef + "  (" + (ef - sf + 1) + "f)";
        var rowText = " [" + items[i].index + "]   " + compName + "   \u2192   " + frameStr;
        lb.add("item", rowText);
    }

    var pg = dlg.add("group"); pg.orientation = "row";
    pg.add("statictext", undefined, "Priority (0\u201310):");
    var prioIn = pg.add("edittext", [0, 0, 48, 22], "5");

    dlg.add("panel", undefined, "");
    var bg = dlg.add("group"); bg.orientation = "row"; bg.alignment = "right";
    bg.add("button", undefined, "Cancel").onClick = function () { dlg.close(2); };
    bg.add("button", undefined, "\u25BA  Submit to Farm").onClick = function () { dlg.close(1); };

    dlg._prio = prioIn;
    return dlg;
}

// =============================================================================
// MAIN
// =============================================================================

function main() {
    var proj = app.project;
    if (!proj) { alert("No project open."); return; }

    // =========================================================================
    // CRITICAL FIX: If project is unsaved, proj.file is null.
    // Calling proj.file.name throws "undefined is not an object".
    // =========================================================================
    if (!proj.file) {
        alert("Please save your project first before submitting.");
        return;
    }

    var farmRoot = normPath(FARM_ROOT);
    var farmFolder = new Folder(farmRoot);
    if (!farmFolder.exists) {
        alert("Farm root not found:\n" + farmRoot + "\n\nCheck that the share is mounted.");
        return;
    }

    var rqi = proj.renderQueue;
    var items = [];
    for (var i = 1; i <= rqi.numItems; i++) {
        var item = rqi.item(i);
        if (item.status === RQItemStatus.QUEUED) items.push({ index: i, item: item });
    }
    if (!items.length) {
        alert("No QUEUED items in the Render Queue.\nSet at least one item to Queued before running this script.");
        return;
    }

    var dlg = showDialog(items);
    if (dlg.show() !== 1) return;

    var priority = parseInt(dlg._prio.text, 10);
    if (isNaN(priority) || priority < 0) priority = 0;
    if (priority > 10) priority = 10;

    var hostname = "";
    try { hostname = system.callSystem("hostname").replace(/[\r\n\s]/g, ""); } catch (e) { }
    if (!hostname) hostname = "UNKNOWN";

    var submitter = "";
    try { submitter = system.getUserName ? system.getUserName() : hostname; } catch (e) { }
    if (!submitter) submitter = hostname;

    var projPath = proj.file.fsName;
    var runId = makeJobId();
    var dateStr = getDateStr();

    makeDirs(pjoin(farmRoot, "jobs"));
    makeDirs(pjoin(farmRoot, "renders"));
    makeDirs(pjoin(farmRoot, "projects"));

    app.beginSuppressDialogs();

    var remaps = remapToUNC(proj, hostname);
    var newPaths = [];

    for (var ii = 0; ii < items.length; ii++) {
        var rqItem = items[ii].item;
        var comp = rqItem.comp;
        if (!comp) { newPaths.push(null); continue; }

        try { rqItem.applyTemplate("Multi-Machine Settings"); } catch (e) { }
        try { rqItem.skipExistingFiles = true; } catch (e) { }

        var origExt = ".png";
        try {
            if (rqItem.numOutputModules > 0) {
                var om = rqItem.outputModule(1);
                if (om && om.file) {
                    var cleanFn = om.file.name.replace(/\[#+\]/g, "").replace(/_+#+/g, "").replace(/#+#+/g, "");
                    var di = cleanFn.lastIndexOf(".");
                    if (di !== -1) origExt = cleanFn.slice(di);
                }
            }
        } catch (e) { }

        var safe = safeName(comp.name);
        var renderDir = pjoin(pjoin(pjoin(farmRoot, "renders"), dateStr), safe);
        makeDirs(renderDir);
        var outFile = pjoin(renderDir, safe + "_[#####]" + origExt);
        newPaths.push(outFile);

        try {
            if (rqItem.numOutputModules > 0)
                rqItem.outputModule(1).file = new File(outFile);
        } catch (e) { }
    }

    var cleanProjName = proj.file.name.replace(/\.aep$/i, "").replace(/_FARM_[A-Z0-9]*/ig, "").replace(/[^a-zA-Z0-9_\-]/g, "_");
    var projCopyPath = pjoin(pjoin(farmRoot, "projects"), cleanProjName + "_FARM_" + runId + ".aep");
    var farmCopyOK = false;
    try { proj.save(new File(projCopyPath)); farmCopyOK = true; } catch (e) { }

    restoreLocalPaths(proj, remaps);
    app.endSuppressDialogs(false);

    var jobsDir = pjoin(farmRoot, "jobs");
    var submitted = 0;
    var errors = [];

    for (var ii = 0; ii < items.length; ii++) {
        var rqItem = items[ii].item;
        var rqIndex = items[ii].index;
        var comp = rqItem.comp;

        if (!comp || !newPaths[ii]) {
            errors.push("Item " + items[ii].index + ": skipped (no comp / no output path)");
            continue;
        }

        var sf = 0; var ef = 0;
        try {
            sf = Math.round(rqItem.timeSpanStart / comp.frameDuration);
            ef = Math.round(sf + rqItem.timeSpanDuration / comp.frameDuration) - 1;
        } catch (e) { }

        var outPath = newPaths[ii];
        var outType = detectType(outPath);
        var finalProj = farmCopyOK ? projCopyPath : toUNC(projPath, hostname);
        var usedPlugins = getUsedPlugins(comp);
        var jobId = runId + "_" + rqIndex;
        var jobFile = pjoin(jobsDir, jobId + ".json");

        var manifest = {
            job_id: jobId,
            status: "PENDING",
            comp_name: comp.name,
            project_path: finalProj,
            output_path: outPath,
            output_folder: parentDir(outPath),
            output_ext: getExt(outPath),
            output_type: outType,
            start_frame: sf,
            end_frame: ef,
            total_frames: (ef - sf + 1),
            fps: comp.frameRate,
            width: comp.width,
            height: comp.height,
            rq_index: rqIndex,
            priority: priority,
            submitted_by: submitter,
            submitted_from: hostname,
            submitted_at: nowStr(),
            submitted_epoch: epochNow(),
            is_video: (outType === "VIDEO"),
            required_plugins: usedPlugins,
            input_path_audit: remaps,
            farm_root: farmRoot,
            submit_version: SUBMIT_VER
        };

        if (writeJSON(jobFile, manifest)) { submitted++; }
        else { errors.push("Item " + rqIndex + ": FAILED to write job file -> " + jobFile); }
    }

    var msg = "Submitted " + submitted + " of " + items.length + " job(s).\n\n";
    msg += "Farm root  :  " + farmRoot + "\n";
    msg += "Output     :  renders\\" + dateStr + "\\CompName\\...\n";
    if (farmCopyOK) {
        msg += "Project    :  " + projCopyPath + "\n";
    } else {
        msg += "Project    :  WARNING \u2014 farm .aep copy FAILED\n";
        msg += "             Slaves will try:  " + toUNC(projPath, hostname) + "\n";
        msg += "\n\u26A0  Check write access to:  " + pjoin(farmRoot, "projects") + "\n";
    }
    if (errors.length) msg += "\nErrors:\n" + errors.join("\n");
    msg += "\n\nOpen AEREN_Slave.py on each render machine and click Render.";
    alert(msg);
}

main();