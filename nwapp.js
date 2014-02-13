function run() {
	var gui = require('nw.gui');
	var win = gui.Window.get();
	win.showDevTools();

	var ctrlfreq = require('./ctrlfreq');
	var _ = require('underscore')
	var Q = require('q');
	var fs = require('fs');

	var dbpath = "./db/chunks";
	var g_store = null;

	console.log("Opening store at " + dbpath);
	var leveldbstore = require('./leveldb_store');
	leveldbstore.store(dbpath).then(function(store) {
		console.log("Store opened")
		$('#holder').show('slow');
		g_store = store;
	}).done();



	// prevent default behavior from changing page on dropped file
	window.ondragover = function(e) {
		e.preventDefault();
		return false
	};
	window.ondrop = function(e) {
		e.preventDefault();
		return false
	};

	var holder = document.getElementById('holder');
	holder.ondragover = function() {
		this.className = 'hover';
		return false;
	};
	holder.ondragend = function() {
		this.className = '';
		return false;
	};


	holder.ondrop = function(e) {
		e.preventDefault();
		var files = e.dataTransfer.files;
		$('#files').html("");

		try {
			var paths = [];
			$.each(files, function(i, file) {
				var file = file.path;
				try {
					fs.stat(file, function(err, stat) {
						if (err) {
							return;
						}
						if (stat && stat.isDirectory()) {
							var backupdiv = $('<div><h2>Backup of ' + file + '</h2><div class="curfile"></div><div class="chunk"></div><ul class="info"></ul></div>');
							$('#backups').append(backupdiv);

							function info(i) {
								var li = $('<li />');
								li.html(i);
								backupdiv.find('.info').append(li);
							}
							var curfile = function(file) {
								backupdiv.find('.curfile').html(file);
							}
							curfile = _.throttle(curfile, 100);
							var chunk = function(file, pos, size) {
								var d = backupdiv.find('.chunk');
								if (pos == 0) {
									d.html("");
								} else {
									d.html(pos.toString() + " of " + size);
								}
							}
							chunk = _.throttle(chunk, 100);

							info("Backing up " + file);
							var backup = ctrlfreq.backup(file, g_store);
							backup.emitter.on('file', curfile);
							backup.emitter.on('file', info);
							backup.emitter.on('filechunk', chunk);
							backup.promise.then(function() {
								console.log("backup promise resolved")
								info("Done backing up " + file);
							}).done();
						}
					});
				} catch (e) {
					console.log("Exception " + e);
				}

			});
		} catch (e) {
			console.log("Exception: " + e);
		}

		return false;
	};
}
$(run);