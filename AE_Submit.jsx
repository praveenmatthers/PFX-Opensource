// ============================================================
// AE_Submit.jsx  —  AEREN Render Farm Submission Script
// Version : 3.0.0  |  AEREN - 2026
// Usage   : File > Scripts > Run Script File  inside After Effects
// ============================================================

(function AE_Submit() {

    var DEFAULT_HOST = "127.0.0.1";
    var DEFAULT_PORT = 9876;
    var WATCH_DIR = Folder.temp.fsName;
    var SEP = ($.os.indexOf("Windows") !== -1) ? "\\" : "/";

    // ── Helpers ───────────────────────────────────────────────────────────────
    function pad(n) { return n < 10 ? "0" + n : "" + n; }

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

    function timestamp() {
        var d = new Date();
        return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
            " " + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    }

    function getHostname() {
        return $.getenv("COMPUTERNAME") || $.getenv("HOSTNAME") || "unknown-host";
    }
    function getUsername() {
        return $.getenv("USERNAME") || $.getenv("USER") || $.getenv("LOGNAME") || "user";
    }

    function isLocalPath(p) {
        if (/^[A-Za-z]:\\/.test(p)) return true;
        if (/^\/(Users|home|Volumes\/Macintosh)/.test(p)) return true;
        return false;
    }

    function localToUNC(p, host) {
        var m = p.match(/^([A-Za-z]):\\(.*)$/);
        if (m) return "\\\\" + host + "\\" + m[1].toUpperCase() + "$\\" + m[2];
        return p;
    }

    // AE sometimes URL-encodes spaces as %20 in fsName on Windows
    function decodePath(p) {
        try { return decodeURIComponent(String(p).replace(/\+/g, " ")); }
        catch (e) { return String(p); }
    }

    // ── Scan effects / plugins used in every comp ─────────────────────────────
    function scanUsedEffects(proj) {
        var seen = {};
        for (var i = 1; i <= proj.numItems; i++) {
            var item = proj.item(i);
            if (!(item instanceof CompItem)) continue;
            for (var j = 1; j <= item.numLayers; j++) {
                var layer = item.layer(j);
                try {
                    var efx = layer.effect;
                    for (var k = 1; k <= efx.numProperties; k++) {
                        try {
                            var ep = efx(k);
                            var mn = ""; try { mn = ep.matchName; } catch (e2) { }
                            var dn = ""; try { dn = ep.name; } catch (e3) { }
                            var key = mn || dn;
                            if (key) seen[key] = { matchName: mn, displayName: dn };
                        } catch (e4) { }
                    }
                } catch (e) { }
            }
        }
        var arr = [];
        for (var n in seen) { if (seen.hasOwnProperty(n)) arr.push(seen[n]); }
        return arr;
    }

    // ── Remap local footage paths → UNC ──────────────────────────────────────
    function remapLocalPaths(proj, hostname) {
        var remaps = [];
        for (var i = 1; i <= proj.numItems; i++) {
            var item = proj.item(i);
            if (!(item instanceof FootageItem)) continue;
            if (!(item.mainSource instanceof FileSource)) continue;
            var orig = item.mainSource.file ? item.mainSource.file.fsName : "";
            if (orig && isLocalPath(orig)) {
                var unc = localToUNC(orig, hostname);
                remaps.push({ item_id: i, orig: orig, unc: unc });
                try { item.replace(new File(unc)); } catch (e) { }
            }
        }
        return remaps;
    }

    function restoreOriginalPaths(proj, remaps) {
        for (var i = 0; i < remaps.length; i++) {
            try {
                var it = proj.item(remaps[i].item_id);
                if (it) it.replace(new File(remaps[i].orig));
            } catch (e) { }
        }
    }

    // ── Pre-flight ────────────────────────────────────────────────────────────
    var proj = app.project;
    if (!proj) { alert("No project is open."); return; }
    if (!proj.file) { alert("Save your project before submitting to AEREN."); return; }

    var rq = proj.renderQueue;
    if (rq.numItems === 0) {
        alert("Render Queue is empty.\nAdd compositions to the Render Queue first.");
        return;
    }

    var hostname = getHostname();
    var username = getUsername();
    var now = new Date();
    var ts = timestamp();
    var requiredEffects = scanUsedEffects(proj);

    // Save render-farm copy with UNC-remapped footage paths
    var footageRemaps = remapLocalPaths(proj, hostname);
    var origFile = proj.file;

    // Decode both folder and filename — AE fsName can contain %20 for spaces on Windows
    var origParentDir = decodePath(origFile.parent.fsName);
    var origFileName = decodePath(origFile.name);

    // Strip any stacked _RENDERFARM suffixes, then append exactly one
    var baseName = origFileName.replace(/\.aep$/i, "").replace(/(_RENDERFARM)+$/i, "");
    var copyName = baseName + "_RENDERFARM.aep";
    var copyFsPath = origParentDir + SEP + copyName;
    var copyFile = new File(copyFsPath);

    try {
        app.project.save(copyFile);
    } catch (e) {
        restoreOriginalPaths(proj, footageRemaps);
        alert("Failed to save render-farm copy:\n" + e.toString());
        return;
    }
    restoreOriginalPaths(proj, footageRemaps);

    // Re-read the actual saved path from AE after save and decode it
    // (AE may re-encode the path internally — this gives us the real filesystem path)
    if (app.project.file) {
        copyFsPath = decodePath(app.project.file.fsName);
    }

    // ── Collect queued render items ───────────────────────────────────────────
    var VIDEO_EXTS = ".mov.mp4.avi.mxf.mkv.wmv.m4v.mpg.mpeg.r3d.braw.f4v.ts.mts.m2ts.flv.webm";
    var jobs = [];

    for (var qi = 1; qi <= rq.numItems; qi++) {
        var rqItem = rq.item(qi);
        if (rqItem.status !== RQItemStatus.QUEUED &&
            rqItem.status !== RQItemStatus.WILL_CONTINUE) continue;

        var comp = rqItem.comp;
        var fps = comp.frameRate;
        var sf = Math.round(comp.workAreaStart * fps);
        var ef = Math.round((comp.workAreaStart + comp.workAreaDuration) * fps) - 1;

        var outPath = "";
        try {
            var omFile = rqItem.outputModule(1).file;
            if (omFile) outPath = decodePath(omFile.fsName);
        } catch (e) { }

        var outFinal = outPath;
        if (outPath && isLocalPath(outPath)) outFinal = localToUNC(outPath, hostname);

        var extMatch = outPath.match(/\.[^.\\\/]+$/);
        var isVideo = !!(extMatch && VIDEO_EXTS.indexOf(extMatch[0].toLowerCase()) >= 0);

        jobs.push({
            comp_name: comp.name,
            project_path: copyFsPath,
            output_path: outFinal,
            output_path_orig: outPath,
            start_frame: sf,
            end_frame: ef,
            fps: fps,
            width: comp.width,
            height: comp.height,
            duration_frames: ef - sf + 1,
            rq_index: qi,
            hostname: hostname,
            is_video: isVideo
        });
    }

    if (jobs.length === 0) {
        alert("No QUEUED items found in the Render Queue.");
        return;
    }

    // ── UI ────────────────────────────────────────────────────────────────────
    var dlg = new Window("dialog", "AEREN  \u2014  Submit Render Job v3.0", undefined);
    dlg.orientation = "column";
    dlg.alignChildren = ["fill", "top"];
    dlg.spacing = 10;
    dlg.margins = 16;

    var titleGrp = dlg.add("group");
    titleGrp.orientation = "row";
    titleGrp.alignChildren = ["left", "center"];
    var titleLbl = titleGrp.add("statictext", undefined,
        "AEREN  Render Manager  \u2014  Job Submission");
    titleLbl.graphics.font = ScriptUI.newFont("Arial", "BOLD", 13);

    dlg.add("panel", undefined, "").preferredSize.height = 1;

    // Project info panel
    var pp = dlg.add("panel", undefined, "Project");
    pp.orientation = "column"; pp.alignChildren = ["fill", "top"];
    pp.margins = 10; pp.spacing = 4;
    pp.add("statictext", undefined, "File      :  " + origFile.name);
    pp.add("statictext", undefined, "Farm copy :  " + copyFsPath);
    pp.add("statictext", undefined,
        "Submitted :  " + ts + "   User: " + username + " @ " + hostname);
    pp.add("statictext", undefined,
        "Plugins   :  " + requiredEffects.length + " unique effect(s) scanned for preflight");

    // Jobs panel
    var jp = dlg.add("panel", undefined, "Queued Render Items  (" + jobs.length + ")");
    jp.orientation = "column"; jp.alignChildren = ["fill", "top"];
    jp.margins = 10; jp.spacing = 6;

    for (var j = 0; j < jobs.length; j++) {
        var jb = jobs[j];
        var jGrp = jp.add("group");
        jGrp.orientation = "column"; jGrp.alignChildren = ["fill", "top"]; jGrp.spacing = 2;

        var hdr = jGrp.add("group");
        var numLbl = hdr.add("statictext", undefined, "#" + jb.rq_index + "  " + jb.comp_name);
        numLbl.graphics.font = ScriptUI.newFont("Arial", "BOLD", 11);
        if (jb.is_video) {
            var vLbl = hdr.add("statictext", undefined, "  [VIDEO \u2014 Single Machine Only]");
            vLbl.graphics.foregroundColor =
                vLbl.graphics.newPen(vLbl.graphics.PenType.SOLID_COLOR, [1, 0.6, 0, 1], 1);
        }
        jGrp.add("statictext", undefined,
            "  " + jb.width + "\xd7" + jb.height +
            "  |  " + jb.fps.toFixed(3) + " fps" +
            "  |  Frames " + jb.start_frame + " \u2013 " + jb.end_frame +
            "  (" + jb.duration_frames + " frames)");
        var outLbl = jGrp.add("statictext", undefined,
            "  Output: " + (jb.output_path || "(not set)"));
        outLbl.graphics.font = ScriptUI.newFont("Arial", "ITALIC", 10);
        if (j < jobs.length - 1)
            jp.add("panel", undefined, "").preferredSize.height = 1;
    }

    // Connection panel
    var conn = dlg.add("panel", undefined, "Render Manager Connection");
    conn.orientation = "row"; conn.alignChildren = ["left", "center"];
    conn.margins = 10; conn.spacing = 10;
    conn.add("statictext", undefined, "Host:");
    var hostEdit = conn.add("edittext", undefined, DEFAULT_HOST);
    hostEdit.preferredSize.width = 130;
    conn.add("statictext", undefined, "Port:");
    var portEdit = conn.add("edittext", undefined, String(DEFAULT_PORT));
    portEdit.preferredSize.width = 60;

    // Options panel
    var optRow = dlg.add("panel", undefined, "Options");
    optRow.orientation = "row"; optRow.alignChildren = ["left", "center"];
    optRow.margins = 10; optRow.spacing = 14;
    optRow.add("statictext", undefined, "Priority (0-10):");
    var prioEdit = optRow.add("edittext", undefined, "5");
    prioEdit.preferredSize.width = 45;
    optRow.add("statictext", undefined, "Chunk Size:");
    var chunkEdit = optRow.add("edittext", undefined, "10");
    chunkEdit.preferredSize.width = 45;
    optRow.add("statictext", undefined, "frames / chunk");

    // Buttons
    var btnRow = dlg.add("group");
    btnRow.alignment = "right"; btnRow.spacing = 8;
    var cancelBtn = btnRow.add("button", undefined, "Cancel");
    var submitBtn = btnRow.add("button", undefined, "  Submit to AEREN  ");
    submitBtn.graphics.font = ScriptUI.newFont("Arial", "BOLD", 12);

    cancelBtn.onClick = function () { dlg.close(); };

    submitBtn.onClick = function () {
        var mHost = (hostEdit.text || DEFAULT_HOST).replace(/\s/g, "");
        if (!/^[a-zA-Z0-9.-]+$/.test(mHost)) {
            alert("Invalid Host format. Please use a valid hostname or IP address.");
            return;
        }

        var mPort = parseInt(portEdit.text, 10) || DEFAULT_PORT;
        var prio = parseInt(prioEdit.text, 10);
        if (isNaN(prio) || prio < 0) prio = 0;
        if (prio > 10) prio = 10;
        var chunkSize = parseInt(chunkEdit.text, 10) || 10;

        var effArr = [];
        for (var ei = 0; ei < requiredEffects.length; ei++) {
            effArr.push({
                matchName: requiredEffects[ei].matchName,
                displayName: requiredEffects[ei].displayName
            });
        }

        var jobsArr = [];
        for (var k = 0; k < jobs.length; k++) {
            var jb = jobs[k];
            jobsArr.push({
                comp_name: jb.comp_name,
                project_path: jb.project_path,
                output_path: jb.output_path,
                output_path_orig: jb.output_path_orig,
                start_frame: jb.start_frame,
                end_frame: jb.end_frame,
                fps: jb.fps,
                width: jb.width,
                height: jb.height,
                duration_frames: jb.duration_frames,
                rq_index: jb.rq_index,
                hostname: jb.hostname,
                is_video: !!jb.is_video
            });
        }

        var payloadObj = {
            submitted_at: ts,
            machine: hostname,
            user: username,
            project: copyFsPath,
            priority: prio,
            chunk_size: chunkSize,
            required_effects: effArr,
            jobs: jobsArr
        };

        var payload = toJSON(payloadObj);

        // Write drop-file for manager file-watcher
        var tmpPath = WATCH_DIR + SEP + "ae_render_job_" + now.getTime() + ".json";
        var tmpFile = new File(tmpPath);
        tmpFile.open("w");
        tmpFile.write(payload);
        tmpFile.close();

        // Fire HTTP POST via curl (best-effort)
        var curlTmp = new File(WATCH_DIR + SEP + "ae_curl_payload.json");
        curlTmp.open("w"); curlTmp.write(payload); curlTmp.close();
        var mURL = "http://" + mHost + ":" + mPort + "/submit";

        if ($.os.indexOf("Windows") !== -1) {
            var bat = new File(WATCH_DIR + "\\ae_submit.bat");
            bat.open("w");
            bat.writeln('@echo off');
            bat.writeln('curl -s -m 5 -X POST -H "Content-Type: application/json"'
                + ' --data-binary @"' + curlTmp.fsName.replace(/\//g, "\\") + '"'
                + ' "' + mURL + '" > NUL 2>&1');
            bat.close();
            bat.execute();
        } else {
            var sh = new File(WATCH_DIR + "/ae_submit.sh");
            sh.open("w");
            sh.writeln("#!/bin/bash");
            sh.writeln("curl -s -m 5 -X POST -H 'Content-Type: application/json'"
                + " --data-binary '@" + curlTmp.fsName + "' '"
                + mURL + "' >/dev/null 2>&1");
            sh.close();
            sh.execute();
        }

        var msg = "\u2713  " + jobs.length + " job(s) submitted to AEREN!\n\n"
            + "Farm Copy  : " + copyFsPath + "\n"
            + "Manager    : " + mHost + ":" + mPort + "\n"
            + "Priority   : " + prio + "\n"
            + "Chunk Size : " + chunkSize + " frames\n"
            + "Plugins    : " + requiredEffects.length + " detected\n\n";

        if (footageRemaps.length > 0) {
            msg += "Remapped " + footageRemaps.length
                + " local footage path(s) to UNC for network access.\n";
            for (var si = 0; si < Math.min(footageRemaps.length, 4); si++) {
                msg += "  " + footageRemaps[si].orig + "\n  \u2192 " + footageRemaps[si].unc + "\n";
            }
            if (footageRemaps.length > 4)
                msg += "  ... and " + (footageRemaps.length - 4) + " more.";
        } else {
            msg += "No local footage paths detected (all network/server paths).";
        }

        alert(msg);
        dlg.close();
    };

    dlg.center();
    dlg.show();

})();
