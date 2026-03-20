// ===========================================================================
//  AE_Submit.jsx  —  AEREN Render Farm Job Submission  v5.0.0
//  File-based. Writes job JSON directly to the shared network folder.
//  No IP address. No ports. No internet. No IT dept needed.
//
//  HOW TO USE:
//   1. Set FARM_ROOT below to your shared network path.
//   2. Add your comp(s) to After Effects Render Queue.
//   3. Run: File > Scripts > Run Script File > AE_Submit.jsx
// ===========================================================================

// ═══════════════════════════════════════════════════════════════════════
//  ▶  ONLY SETTING YOU NEED TO CHANGE
var FARM_ROOT = "\\\\DESKTOP-3BK9PQH\\Projects\\AE_RenderManager\\AEREN_DATA_LOGS";
// ═══════════════════════════════════════════════════════════════════════

var SUBMIT_VERSION = "5.0.0";

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------
function padLeft(n, w) {
    var s = String(n);
    while (s.length < w) s = "0" + s;
    return s;
}

function makeJobId() {
    var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var id = "";
    for (var i = 0; i < 8; i++) id += chars.charAt(Math.floor(Math.random() * chars.length));
    return id;
}

function nowStr() {
    var d = new Date();
    return d.getFullYear() + "-" +
        padLeft(d.getMonth() + 1, 2) + "-" +
        padLeft(d.getDate(), 2) + " " +
        padLeft(d.getHours(), 2) + ":" +
        padLeft(d.getMinutes(), 2) + ":" +
        padLeft(d.getSeconds(), 2);
}

function epochNow() {
    return Math.floor(new Date().getTime() / 1000);
}

function fileExists(path) {
    var f = new File(path);
    return f.exists;
}

function writeJSON(filePath, obj) {
    var f = new File(filePath);
    f.encoding = "UTF-8";
    if (f.open("w")) {
        f.write(JSON.stringify(obj, null, 2));
        f.close();
        return true;
    }
    return false;
}

function copyFile(srcPath, dstPath) {
    var src = new File(srcPath);
    var dst = new File(dstPath);
    return src.copy(dst.fsName);
}

function makeDirs(path) {
    var f = new Folder(path);
    if (!f.exists) f.create();
}

// Output type detection
function detectOutputType(outputPath) {
    var ext = outputPath.split('.').pop().toLowerCase();
    if (ext === "mov" || ext === "mp4" || ext === "avi" || ext === "mxf" || ext === "r3d") {
        return "VIDEO";
    }
    return "SEQUENCE";
}

// Detect installed AE plugin names
function getInstalledPlugins() {
    var plugins = [];
    try {
        var plugDir = new Folder(app.path + "/Plug-ins");
        if (plugDir.exists) {
            var files = plugDir.getFiles("*.aex");
            for (var i = 0; i < files.length; i++) {
                plugins.push(files[i].displayName.replace(".aex", ""));
            }
        }
    } catch (e) { }
    return plugins;
}

// Convert local path like D:\Projects\Foo.aep to UNC \\HOSTNAME\D$\Projects\Foo.aep
function localToUNC(path, hostname) {
    var unc = path.replace(/([A-Za-z]):\\/, "\\\\" + hostname + "\\$1$\\");
    return unc.split("/").join("\\");
}

function remapLocalPaths(proj, hostname) {
    var remaps = [];
    for (var i = 1; i <= proj.numItems; i++) {
        var item = proj.item(i);
        if (!(item instanceof FootageItem)) continue;
        if (!(item.mainSource instanceof FileSource)) continue;
        var orig = item.mainSource.file ? item.mainSource.file.fsName : "";
        if (orig && /^[A-Za-z]:\\/.test(orig)) {
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

// ---------------------------------------------------------------------------
//  Main Script
// ---------------------------------------------------------------------------
function main() {

    var proj = app.project;
    if (!proj) { alert("No project open."); return; }
    if (!proj.file) { alert("Please save your project before submitting."); return; }

    // Check farm root is accessible
    var farmFolder = new Folder(FARM_ROOT);
    if (!farmFolder.exists) {
        alert("Farm root not found:\n" + FARM_ROOT +
            "\n\nMake sure the network share is connected and FARM_ROOT is correct.");
        return;
    }

    var jobsDir = new Folder(FARM_ROOT + "\\jobs");
    makeDirs(jobsDir.fsName);

    // Collect checked render queue items
    var rqi = proj.renderQueue;
    var items = [];
    for (var i = 1; i <= rqi.numItems; i++) {
        var item = rqi.item(i);
        if (item.status === RQItemStatus.QUEUED || item.status === RQItemStatus.UNRENDERED) {
            items.push({ index: i, item: item });
        }
    }

    if (items.length === 0) {
        alert("No queued render items found.\n\nAdd items to the Render Queue and set them to 'Queued' status.");
        return;
    }

    var hostname = system.callSystem("hostname").replace(/\n|\r/g, "").replace(/^\s+|\s+$/g, "");
    var submitter = system.getUserName ? system.getUserName() : hostname;
    var projPath = proj.file.fsName;
    var plugins = getInstalledPlugins();

    // Generate a single run jobId for this submission batch
    var runJobId = makeJobId();

    // Remap footage paths -> save FARM copy -> restore paths
    app.beginSuppressDialogs();
    var remaps = remapLocalPaths(proj, hostname);

    var projectsDir = new Folder(FARM_ROOT + "\\projects");
    makeDirs(projectsDir.fsName);

    // Strip out any existing _FARM_XXXXXX tags so we never stack them into path overflow
    var cleanName = proj.file.name.replace(/\.aep$/i, "").replace(/_FARM_[A-Z0-9]+/ig, "");
    var projNetPath = FARM_ROOT + "\\projects\\" + cleanName + "_FARM_" + runJobId + ".aep";

    var farmCopySaved = false;
    var farmFile = new File(projNetPath);
    try {
        proj.save(farmFile);
        farmCopySaved = true;
    } catch (e) { }

    restoreOriginalPaths(proj, remaps);
    app.endSuppressDialogs(false);

    // Combined single Dialog for Setting up Job
    var dlg = new Window("dialog", "AEREN Render Submit");

    // Make UI Dark/Black
    try {
        dlg.graphics.backgroundColor = dlg.graphics.newBrush(dlg.graphics.BrushType.SOLID_COLOR, [0.12, 0.12, 0.12, 1]);
    } catch (e) { }

    dlg.orientation = "column";
    dlg.alignChildren = ["fill", "top"];
    dlg.spacing = 15;
    dlg.margins = 20;

    var pnl = dlg.add("panel", undefined, "Farm Settings");
    pnl.orientation = "column";
    pnl.alignChildren = ["left", "center"];
    pnl.spacing = 12;
    pnl.margins = 18;

    // Chunk Size
    var chunkGrp = pnl.add("group");
    chunkGrp.add("statictext", undefined, "Frames per chunk:");
    var chunkIn = chunkGrp.add("edittext", undefined, "5");
    chunkIn.characters = 5;

    // Priority
    var prioGrp = pnl.add("group");
    prioGrp.add("statictext", undefined, "Job Priority (0-10):");
    var prioIn = prioGrp.add("edittext", undefined, "5");
    prioIn.characters = 5;

    // Copyright
    var copyGrp = dlg.add("group");
    copyGrp.alignment = ["center", "bottom"];
    var copyTxt = copyGrp.add("statictext", undefined, "2026 - Copyright Reserved - Praveen Brijwal");
    try {
        copyTxt.graphics.font = ScriptUI.newFont("Arial", "ITALIC", 10);
        copyTxt.graphics.foregroundColor = copyTxt.graphics.newPen(copyTxt.graphics.PenType.SOLID_COLOR, [0.6, 0.6, 0.6, 1], 1);
    } catch (e) { }

    // Buttons
    var btnGrp = dlg.add("group");
    btnGrp.alignment = "right";
    var cancelBtn = btnGrp.add("button", undefined, "Cancel");
    var submitBtn = btnGrp.add("button", undefined, "Submit");

    cancelBtn.onClick = function () { dlg.close(2); };
    submitBtn.onClick = function () { dlg.close(1); };

    if (dlg.show() !== 1) return;

    var chunkSize = parseInt(chunkIn.text) || 5;
    var priority = parseInt(prioIn.text) || 5;

    var submitted = 0;
    var errors = [];

    for (var ii = 0; ii < items.length; ii++) {
        var rqItem = items[ii].item;
        var rqIndex = items[ii].index;

        try {
            var comp = rqItem.comp;
            if (!comp) continue;

            var sf = rqItem.timeSpanStart / comp.frameDuration;
            var ef = sf + (rqItem.timeSpanDuration / comp.frameDuration) - 1;
            sf = Math.round(sf);
            ef = Math.round(ef);

            // Output path from first output module
            var outputPath = "";
            if (rqItem.numOutputModules > 0) {
                outputPath = rqItem.outputModule(1).file ? rqItem.outputModule(1).file.fsName : "";
            }

            var outType = detectOutputType(outputPath);

            // Convert local output path to UNC for farm rendering
            var finalOutput = outputPath;
            if (finalOutput && /^[A-Za-z]:\\/.test(finalOutput)) {
                finalOutput = localToUNC(finalOutput, hostname);
            }

            // Auto-create output directory so aerender doesn't crash
            if (finalOutput) {
                var outFolder = new Folder(finalOutput).parent;
                if (!outFolder.exists) {
                    try { makeDirs(outFolder.fsName); } catch (e) { }
                }
            }

            var finalProject = projPath;
            if (farmCopySaved) {
                finalProject = projNetPath;
            } else {
                finalProject = localToUNC(projPath, hostname);
            }

            // Build job object
            var jobObj = {
                job_id: runJobId + "_" + rqIndex,
                status: "PENDING",
                comp_name: comp.name,
                project_path: finalProject,
                output_path: finalOutput,
                output_type: outType,
                start_frame: sf,
                end_frame: ef,
                total_frames: ef - sf + 1,
                fps: comp.frameRate,
                width: comp.width,
                height: comp.height,
                rq_index: rqIndex,
                priority: priority,
                chunk_size: chunkSize,
                auto_debug: true,
                required_plugins: plugins,
                submitted_by: submitter,
                submitted_from: hostname,
                submitted_at: nowStr(),
                submitted_epoch: epochNow(),
                project_copy_source: projPath,
                submit_version: SUBMIT_VERSION
            };

            var jobFile = FARM_ROOT + "\\jobs\\JOB_" + runJobId + "_" + rqIndex + ".json";
            if (writeJSON(jobFile, jobObj)) {
                submitted++;
                // Optional: mark the render queue item as rendering to avoid accidental re-submit
                // rqItem.status = RQItemStatus.WILL_CONTINUE; // commented out — let AE manage
            } else {
                errors.push("Failed to write: " + jobFile);
            }

        } catch (e) {
            errors.push("Item " + items[ii].index + ": " + e.message);
        }
    }

    // Result
    var msg = "Submitted " + submitted + " job(s) to render farm.\n\n";
    msg += "Farm: " + FARM_ROOT + "\n";
    msg += "Open AE_RenderManager.py to approve and start rendering.\n";
    if (errors.length > 0) msg += "\nErrors:\n" + errors.join("\n");
    alert(msg);
}

// Run
main();
