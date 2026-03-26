/*
AE_Collector_ExportManifest.jsx
Version: 0.3.0
*/

(function AE_Collector_ExportManifest() {
    function pad(n) { return (n < 10 ? '0' : '') + n; }
    function isoLocal(d) {
        return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    }
    function safeName(s) {
        return String(s).replace(/[\\\/:*?"<>|]+/g, '_').replace(/^\s+|\s+$/g, '');
    }
    function ensureFolder(folderObj) {
        if (!folderObj.exists) {
            if (!folderObj.create()) {
                throw new Error('Could not create folder: ' + folderObj.fsName);
            }
        }
    }
    function escapeString(s) {
        return String(s)
            .replace(/\\/g, '\\\\')
            .replace(/"/g, '\\"')
            .replace(/\r/g, '\\r')
            .replace(/\n/g, '\\n')
            .replace(/\t/g, '\\t');
    }
    function stringify(value, indent, level) {
        indent = indent || '  ';
        level = level || 0;
        if (value === null) return 'null';
        var t = typeof value;
        if (t === 'string') return '"' + escapeString(value) + '"';
        if (t === 'number') return isFinite(value) ? String(value) : 'null';
        if (t === 'boolean') return value ? 'true' : 'false';
        if (value instanceof Array) {
            if (value.length === 0) return '[]';
            var arrParts = [];
            for (var i = 0; i < value.length; i++) {
                arrParts.push(new Array(level + 2).join(indent) + stringify(value[i], indent, level + 1));
            }
            return '[\n' + arrParts.join(',\n') + '\n' + new Array(level + 1).join(indent) + ']';
        }
        var keys = [];
        for (var k in value) {
            if (value.hasOwnProperty(k)) keys.push(k);
        }
        if (keys.length === 0) return '{}';
        var objParts = [];
        for (var j = 0; j < keys.length; j++) {
            var key = keys[j];
            objParts.push(new Array(level + 2).join(indent) + '"' + escapeString(key) + '": ' + stringify(value[key], indent, level + 1));
        }
        return '{\n' + objParts.join(',\n') + '\n' + new Array(level + 1).join(indent) + '}';
    }

    function getSelectedRootComps() {
        var comps = [];
        if (app.project && app.project.selection && app.project.selection.length > 0) {
            for (var i = 0; i < app.project.selection.length; i++) {
                var it = app.project.selection[i];
                if (it instanceof CompItem) comps.push(it);
            }
        }
        return comps;
    }

    function getActiveComp() {
        try {
            if (app.project && app.project.activeItem && app.project.activeItem instanceof CompItem) {
                return app.project.activeItem;
            }
        } catch (e) { }
        return null;
    }

    function tryGetFileInfos(footageItem) {
        var infos = [];
        function addSource(srcObj, isProxy) {
            try {
                if (srcObj && srcObj.file) {
                    var nm = srcObj.file.name;
                    var extMatch = /\.([^.]+)$/.exec(nm);
                    var ext = extMatch ? extMatch[1].toLowerCase() : null;
                    // Prevent common video formats from being treated as sequences
                    var isVideo = /^(mov|mp4|avi|wmv|mxf|mkv|webm|flv|r3d|braw)$/i.test(ext || '');
                    var isSeq = false;
                    try {
                        // isStill is false for both video and sequences, so we ensure it has numbers at the end of the stem
                        isSeq = (srcObj.isStill === false) && !isVideo && /(\d+)$/.test(nm.replace(/\.[^.]+$/, ''));
                    } catch (e1) { }

                    infos.push({
                        has_file: true,
                        source_path: srcObj.file.fsName,
                        exists: srcObj.file.exists,
                        is_missing: !srcObj.file.exists,
                        is_sequence_like: isSeq,
                        ext: ext,
                        is_proxy: isProxy,
                        note: null
                    });
                }
            } catch (e2) {
                infos.push({ has_file: false, note: String(e2) });
            }
        }

        addSource(footageItem.mainSource, false);
        if (footageItem.useProxy && footageItem.proxySource) {
            addSource(footageItem.proxySource, true);
        }
        return infos;
    }

    function addUsage(usageMap, key, compName) {
        if (!usageMap[key]) usageMap[key] = {};
        usageMap[key][compName] = true;
    }

    function usageMapToList(m) {
        var out = [];
        for (var k in m) {
            if (m.hasOwnProperty(k)) out.push(k);
        }
        out.sort();
        return out;
    }

    function collectAllFileFootage() {
        var items = [];
        for (var i = 1; i <= app.project.numItems; i++) {
            var it = app.project.item(i);
            if (it instanceof FootageItem) {
                items.push({
                    footage: it,
                    usage: { '__ALL_PROJECT__': true }
                });
            }
        }
        return items;
    }

    function collectFromComps(rootComps) {
        var seenComps = {};
        var footageById = {};

        function walkComp(comp, rootName) {
            var compKey = 'comp:' + comp.id;
            if (seenComps[compKey]) return;
            seenComps[compKey] = true;

            for (var li = 1; li <= comp.numLayers; li++) {
                var layer = comp.layer(li);
                var src = null;
                try { src = layer.source; } catch (e0) { src = null; }
                if (!src) continue;

                if (src instanceof FootageItem) {
                    var fid = src.id;
                    if (!footageById[fid]) {
                        footageById[fid] = {
                            footage: src,
                            usage: {}
                        };
                    }
                    addUsage(footageById[fid].usage, rootName, rootName);
                } else if (src instanceof CompItem) {
                    walkComp(src, rootName);
                }
            }
        }

        for (var i = 0; i < rootComps.length; i++) {
            walkComp(rootComps[i], rootComps[i].name);
        }

        var out = [];
        for (var id in footageById) {
            if (footageById.hasOwnProperty(id)) out.push(footageById[id]);
        }
        return out;
    }

    app.beginUndoGroup('AE_Collector Export Manifest');
    try {
        if (!app.project) throw new Error('No project is open.');
        if (app.project.file === null) {
            alert('Please save the After Effects project before running AE_Collector.');
            return;
        }

        var projectFile = app.project.file;
        var projectName = projectFile.name;
        var projectDir = projectFile.parent.fsName;
        var baseDir = new Folder(Folder.myDocuments.fsName + '\\AE_Collector');
        ensureFolder(baseDir);

        var selectedComps = getSelectedRootComps();
        var rootComps = [];
        var exportMode = 'all_project_footage';
        if (selectedComps.length > 0) {
            rootComps = selectedComps;
            exportMode = 'selected_project_panel_comps_recursive';
        } else {
            var activeComp = getActiveComp();
            if (activeComp) {
                rootComps = [activeComp];
                exportMode = 'active_comp_recursive';
            }
        }

        var collected = (rootComps.length > 0) ? collectFromComps(rootComps) : collectAllFileFootage();
        var manifestItems = [];
        for (var i = 0; i < collected.length; i++) {
            var footage = collected[i].footage;
            var infos = tryGetFileInfos(footage);
            for (var j = 0; j < infos.length; j++) {
                var info = infos[j];
                if (!info.has_file) continue;
                manifestItems.push({
                    item_name: footage.name,
                    item_type: 'FootageItem',
                    source_path: info.source_path,
                    exists: info.exists,
                    is_missing: info.is_missing,
                    is_sequence_like: info.is_sequence_like,
                    extension: info.ext,
                    is_proxy: info.is_proxy || false,
                    used_in: usageMapToList(collected[i].usage),
                    comment: info.note
                });
            }
        }

        var roots = [];
        for (var r = 0; r < rootComps.length; r++) roots.push(rootComps[r].name);

        var manifest = {
            tool: 'AE_Collector',
            version: '0.3.0',
            created_at: isoLocal(new Date()),
            project: {
                name: projectName,
                project_path: projectFile.fsName,
                project_dir: projectDir,
                saved: true
            },
            export_info: {
                mode: exportMode,
                root_comps: roots,
                item_count: manifestItems.length
            },
            items: manifestItems
        };

        var manifestFile = new File(baseDir.fsName + '\\manifest.json');
        manifestFile.encoding = 'UTF-8';
        if (!manifestFile.open('w')) throw new Error('Could not open manifest for writing: ' + manifestFile.fsName);
        manifestFile.write(stringify(manifest, '  ', 0));
        manifestFile.close();

        alert('AE_Collector\nManifest saved (overwritten):\n' + manifestFile.fsName + '\n\nFiles: ' + manifestItems.length + '\nMode: ' + exportMode);
    } catch (err) {
        alert('AE_Collector failed: ' + err.toString());
    } finally {
        app.endUndoGroup();
    }
})();