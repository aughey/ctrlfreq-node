var fs = require('fs');
var Q = require('q');
var _ = require('underscore');
var path = require('path');
var crypto = require('crypto');

var levelup = require('levelup');

var dbpath = "./db/chunks";
var db = levelup(dbpath);

function limit(count) {
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
		next.resolve(function(data) {
			count++;
			runnext();
			return data;
		});
	}

	return function(debugmessage) {
		var deferred = Q.defer();
		waiting.push(deferred);
		if (count > 0) {
			runnext();
		} else {
			console.log("Rate limiting: " + debugmessage)
		}
		return deferred.promise;
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

var filelimit = limit(10);
var readlimit = limit(10);

function processfile(fullpath) {
	return filelimit(fullpath).then(function(done) {
		fs.open(fullpath, 'r', function(err, fd) {
			if (err) {
				done(null);
				return;
			}
			var chunks = [];
			console.log(fullpath);

			function readnext() {
				var size = 1048576;
				var buffer = new Buffer(size);
				var outstanding = 0;
				readlimit("Buffer").then(function(bufferdone) {
					fs.read(fd, buffer, 0, size, null, function(err, bytesread, buffer) {
						//console.log("Reading " + fullpath + " " + bytesread)
						if (err) {
							console.log("Error reading from " + fullpath);
							bufferdone();
							done(null);
							fs.close(fd);
							return;
						}
						if (bytesread == 0) {
							fs.close(fd);
							bufferdone();
							done(chunks);
							return;
						}
						buffer = buffer.slice(0, bytesread);
						var index = chunks.length;
						chunks.push(null);
						outstanding += 1;
						storechunk(buffer).then(function(sha) {
							chunks[index] = sha;
							outstanding -= 1;
							if (outstanding == 0) {
								bufferdone();
								done(chunks);
							}
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
	var deferred = Q.defer();
	fs.readdir(dirname, function(err, dirfiles) {
		if (err) {
			console.log("Error reading " + dirname + ": " + err);
			deferred.resolve(null);
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
		Q.all(stats).then(function() {
			Q.all(files).then(function(files) {
				Q.all(dirs).then(function(dirs) {
					deferred.resolve({
						dirs: dirs,
						files: files
					});
				}).done()
			}).done()
		}).done()
	});
	return deferred.promise;
}

var args = process.argv;
args.shift()
args.shift()
console.log(args);
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
	});
});