var fs = require('fs');
var Q = require('q');
var _ = require('underscore');
var path = require('path');
var events = require('events');
var limit = require('./limit').limit;

function numberWithCommas(x) {
	return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

var filecount = 0;
var dircount = 0;
var chunkwrite = 0;

// Larger function to wrap  a single backup system.
function init(store, processfile) {
	var dirlimit = limit(10, "dirlimit");

	var emitter = new events.EventEmitter();

	function processdir(dirname) {
		// We return our own promise
		var deferred = Q.defer();
		dirlimit(dirname).then(function(dirdone) {
			dircount++;
			emitter.emit('dir', dirname);
			fs.readdir(dirname, function(err, dirfiles) {
				if (err) {
					console.log("Error reading " + dirname + ": " + err);
					dirdone(null);
					return;
				}
				var stats = [];
				var files = [];
				var dirs = [];
				_.each(dirfiles, function(file) {
					var fullpath = path.join(dirname, file);

					try {
						stats.push(Q.nfcall(fs.stat, fullpath).then(function(stat) {
							var storestat = _.pick(stat, 'mode', 'uid', 'gid', 'size', 'mtime');
							storestat.mtime = storestat.mtime.toISOString();
							if (err) {
								console.log("Could not stat: " + fullpath);
								return;
							}
							if (stat.isFile()) {
								files.push(processfile(fullpath, file, storestat, store, emitter).then(function(chunks) {
									return {
										name: file,
										stat: storestat,
										chunks: chunks
									};
								}));
							} else if (stat.isDirectory()) {
								delete storestat.size;
								delete storestat.mtime;
								dirs.push(processdir(fullpath).then(function(dirinfo) {
									return store.save(JSON.stringify(dirinfo), null, "Directory " + fullpath).then(function(sha) {
										return {
											name: file,
											stat: storestat,
											sha: sha
										};
									});
								}));
							}
						}));
					} catch (e) {
						console.log("Exception on file: " + fullpath);
						console.log(e);
					}
				});
				//console.log(dirname + " Waiting on stats: " + stats.length);
				Q.all(stats).then(function() {
					//console.log(dirname + " Waiting on files: " + files.length);
					Q.all(files).then(function(files) {
						//console.log(dirname + " Waiting on dirs: " + dirs.length);
						dirdone();
						Q.all(dirs).then(function(dirs) {
							deferred.resolve({
								dirs: _.sortBy(dirs, function(d) {
									return d.name;
								}),
								files: _.sortBy(files, function(f) {
									return f.name;
								})
							});
						}).done();
					}).done();
				}).done();
			});
		});
		return deferred.promise;
	}

	return {
		processdir: processdir,
		emitter: emitter
	};
}

module.exports = {
	backup: function(dir, store, processfile) {
		var bk = init(store, processfile);
		var promise = bk.processdir(dir);
		return {
			emitter: bk.emitter,
			promise: promise
		};
	},
};
