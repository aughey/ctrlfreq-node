var fs = require('fs');
var Q = require('q');
var _ = require('underscore');
var path = require('path');
var crypto = require('crypto');

var levelup = require('levelup');

var dbpath = "./db/chunks";
var db = levelup(dbpath);

function limit(count, dm) {
	var waiting = [];

	function whendone() {
		count++;
		runnext();
	}

	function runnext() {
		if (waiting.length == 0) {
			return;
		}
		var next = waiting.shift();
		count--;
		var deferred = next[0];
		var cb = next[1];
		cb(function(data) {
			count++;
			runnext();
			deferred.resolve(data);
		});
	}

	return function(debugmessage) {
		return {
			then: function(cb) {
				var deferred = Q.defer();
				waiting.push([deferred, cb]);
				if (count > 0) {
					runnext();
				} else {
					console.log("Rate limiting: " + dm + ":" + debugmessage)
				}
				return deferred.promise;
			}
		}
	}
}

function storechunk(buffer) {
	var deferred = Q.defer();
	var shasum = crypto.createHash('sha1');
	shasum.update(buffer);
	var digest = shasum.digest('hex');

	if (true) {
		db.put(digest, buffer, function(err) {
			if (err) {
				console.log("Error writing sha to database: " + err);
				process.exit(1);
				deferred.resolve(null);
				return;
			}
			deferred.resolve(digest);
		});
	} else {
		deferred.resolve(digest);
	}

	return deferred.promise;
}

var dirlimit = limit(10, "dirlimit");
var filelimit = limit(10, "filelimit");
var readlimit = limit(100, "readlimit");

function processfile(fullpath) {
	return filelimit(fullpath).then(function(filedone) {
		fs.open(fullpath, 'r', function(err, fd) {
			if (err) {
				filedone(null);
				return;
			}
			var chunks = [];

			var outstanding = 1;

			function readnext() {
				var size = 1048576;
				var buffer = new Buffer(size);

				function checkdone(error) {
					outstanding -= 1;
					if (outstanding == 0) {
						console.log("file done: " + fullpath);
						filedone(error ? null : chunks);
					}
				}
				//console.log("Trying to read from: " + fullpath)
				readlimit("Buffer").then(function(bufferdone) {
					fs.read(fd, buffer, 0, size, null, function(err, bytesread, buffer) {
						console.log("Reading " + fullpath + " " + bytesread)
						if (err) {
							console.log("Error reading from " + fullpath);
							fs.close(fd);
							bufferdone();
							checkdone(true);
							return;
						}
						if (bytesread == 0) {
							fs.close(fd);
							bufferdone();
							checkdone();
							return;
						}
						buffer = buffer.slice(0, bytesread);
						var index = chunks.length;
						chunks.push(null);
						outstanding += 1;
						storechunk(buffer).then(function(sha) {
							chunks[index] = sha;
							bufferdone();
							checkdone();
						});
						readnext();
					})
				})

			}
			readnext();
		});
	})
}


function processdir(dirname) {
	console.log("reading dir: " + dirname)
	return dirlimit(dirname).then(function(dirdone) {
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
						if (err) {
							console.log("Could not stat: " + fullpath)
							return;
						}
						if (stat.isFile()) {
							files.push(processfile(fullpath).then(function(chunks) {
								return {
									name: file,
									stat: stat,
									chunks: chunks
								}
							}));
						} else if (stat.isDirectory()) {
							dirs.push(processdir(fullpath).then(function(dirinfo) {
								return storechunk(JSON.stringify(dirinfo)).then(function(sha) {
									return {
										name: file,
										stat: stat,
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
			console.log(dirname + " Waiting on stats: " + stats.length);
			Q.all(stats).then(function() {
				console.log(dirname + " Waiting on files: " + files.length);
				Q.all(files).then(function(files) {
					console.log(dirname + " Waiting on dirs: " + dirs.length);
					Q.all(dirs).then(function(dirs) {
						console.log("done with " + dirname)
						dirdone({
							dirs: dirs,
							files: files
						});
					}).done()
				}).done()
			}).done()
		});
	})
}

var args = process.argv;
args.shift()
args.shift()
_.each(args, function(dirpath) {
	processdir(dirpath).then(function(result) {
		console.log("done processing")
		console.log(JSON.stringify(result, null, 3));
		db.put(dirpath, JSON.stringify(result), function() {
			console.log("Closing db")
			db.close(function(err) {
				console.log("Database closed")
				levelup.repair(dbpath, function() {
					console.log("Database " + dbpath + " compacted.")
				})
			});
		});
	}).done();
});