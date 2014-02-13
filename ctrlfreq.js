var fs = require('fs');
var Q = require('q');
var _ = require('underscore');
var path = require('path');
var crypto = require('crypto');

var levelup = require('levelup');

var dbpath = "./db/chunks";

var db = null; // connected before we process directories.

function numberWithCommas(x) {
	return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

var chunkcount = 0;
var filecount = 0;
var cachecount = 0;
var dircount = 0;
var bytecount = 0;
var chunkwrite = 0;
var bytewrite = 0;

var bytespending = 0;
var maxbytes = 0;

function bytecheck(len) {
	bytespending += len;
	if (bytespending > maxbytes) {
		//console.log("Bytes in play: " + bytespending);
		maxbytes = bytespending;
	}
	return function() {
		bytespending -= len;
	}
}

function limit(count, dm) {
	var waiting = [];

	function runnext() {
		if (waiting.length == 0) {
			return;
		}
		var next = waiting.shift();
		var deferred = next[0];
		var cb = next[1];
		cb(function(data) {
			//console.log("Rate limit done: " + dm + " " + waiting.length)
			count++;
			if (waiting.length != 0) {
				count--;
				process.nextTick(runnext);
			}
			deferred.resolve(data);
		});
	}

	return function(debugmessage) {
		return {
			then: function(cb) {
				var deferred = Q.defer();
				waiting.push([deferred, cb]);
				if (count > 0) {
					count--;
					process.nextTick(runnext);
				} else {
					//console.log("Rate limiting: " + dm + ":" + debugmessage + " " + waiting.length)
				}
				return deferred.promise;
			}
		}
	}
}

function noop() {}

function isChunkStored(key, cb) {
	var iterator = db.db.iterator({
		start: key,
		values: false
	});
	iterator.next(function(err, ikey) {
		iterator.end(noop);
		if (!ikey) {
			cb(false);
		}
		if (ikey == key) {
			cb(true);
		} else {
			cb(false);
		}
	})
}

function storechunk(buffer, precomputedkey, debug) {
	var deferred = Q.defer();

	if (precomputedkey) {
		var digest = precomputedkey;
	} else {
		var shasum = crypto.createHash('sha1');
		shasum.update(buffer);
		var digest = shasum.digest('hex');
	}

	chunkcount++;
	bytecount += buffer.length;


	//deferred.resolve(digest);
	//return deferred.promise;

	isChunkStored(digest, function(stored) {
		if (stored) {
			deferred.resolve(digest);
			//console.log("Key: " + digest + " already in database")
		} else {
			//console.log("##STORING key " + digest + " " + debug);
			//console.log(buffer);
			db.put(digest, buffer, function(err) {
				//console.log(" ## done storing key " + digest)
				if (err) {
					console.log("Error writing sha to database: " + err);
					process.exit(1);
					deferred.resolve(null);
					return;
				}
				chunkwrite++;
				bytewrite += buffer.length;
				deferred.resolve(digest);
			});
		}
	});

	return deferred.promise;
}

var dirlimit = limit(10, "dirlimit");
var filelimit = limit(10, "filelimit");
var readlimit = limit(100, "readlimit");

try {
	var filecache = JSON.parse(fs.readFileSync("cache.json"));
	console.log("Loaded filecache")
} catch(e) {
	var filecache = {};
}
var newcache = {};

function processfile(fullpath,filename,stat) {
	filecount++;
	var cache = filecache[fullpath];
	if(cache) {
		//console.log(cache.mtime + " " + stat.mtime.toISOString())
		//console.log(JSON.stringify(stat));

		if(_.isEqual(stat,cache.stat)) {
			cachecount++;
			newcache[fullpath] = cache;
			return Q.fcall(function() {
				return cache.chunks;
			});
		}
	}
	return processfile_full(fullpath).then(function(chunks) {
		newcache[fullpath] = {
			stat: stat,
			chunks: chunks
		};
		return chunks;
	});
}

function processfile_full(fullpath) {
	return filelimit(fullpath).then(function(filedone) {
		fs.open(fullpath, 'r', function(err, fd) {
			if (err) {
				filedone(null);
				return;
			}
			var chunks = [];

			// This is set to 1 because the close calls checkdone.
			var outstanding = 1;

			function readnext() {
				var size = 1048576;
				var buffer = new Buffer(size);

				function checkdone(error) {
					outstanding -= 1;
					if (outstanding == 0) {
						filedone(error ? null : chunks);
					}
				}
				//console.log("Trying to read from: " + fullpath)
				readlimit("Buffer").then(function(bufferdone) {
					fs.read(fd, buffer, 0, size, null, function(err, bytesread, buffer) {
						//fconsole.log("Reading " + fullpath + " " + bytesread)
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
						var unlog = bytecheck(buffer.length);
						var index = chunks.length;
						chunks.push(null);
						outstanding += 1;
						storechunk(buffer,null,fullpath).then(function(sha) {
							unlog();
							chunks[index] = sha;
							bufferdone();
							checkdone();
						}).done();
						readnext();
					})
				})

			}
			readnext();
		});
	})
}


function processdir(dirname) {
	//console.log("reading dir: " + dirname)
	// We return our own promise
	var deferred = Q.defer();
	dirlimit(dirname).then(function(dirdone) {
		dircount++;
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
						var storestat = _.pick(stat,'mode','uid','gid','size','mtime')
						storestat.mtime = storestat.mtime.toISOString();
						if (err) {
							console.log("Could not stat: " + fullpath)
							return;
						}
						if (stat.isFile()) {
							files.push(processfile(fullpath,file,storestat).then(function(chunks) {
								return {
									name: file,
									stat: storestat,
									chunks: chunks
								}
							}));
						} else if (stat.isDirectory()) {
							delete storestat.size;
							delete storestat.mtime;
							dirs.push(processdir(fullpath).then(function(dirinfo) {
								return storechunk(JSON.stringify(dirinfo),null,"Directory " + fullpath).then(function(sha) {
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
							dirs: _.sortBy(dirs,function(d) { return d.name }),
							files: _.sortBy(files,function(f) { return f.name })
						})
					}).done()
				}).done()
			}).done()
		});
	})
	return deferred.promise;
}

var args = process.argv;
args.shift()
args.shift()

Q.nfcall(levelup, dbpath).then(function(d) {
	db = d; // Setting the global db
	return Q.all(_.map(args, function(dirpath) {
		console.log("Kicking off processing: " + dirpath)
		dirpath = path.resolve(dirpath);
		return processdir(dirpath).then(function(result) {
			console.log("done processing directory: " + dirpath)
			var info = JSON.stringify(result, dirpath);
			return storechunk(info,null,"Top directory " + dirpath);
		});
	}));
}).then(function() {
	console.log("Closing db ");
	return Q.ninvoke(db, 'close');
}).then(function() {
	console.log("Database closed")
	//return Q.ninvoke(levelup, 'repair', dbpath);
}).then(function() {
	console.log("Directory Count: " + numberWithCommas(dircount));
	console.log("File Count: " + numberWithCommas(filecount));
	console.log("Cache Hits: " + numberWithCommas(cachecount));
	console.log("Chunk Count: " + numberWithCommas(chunkcount));
	console.log("Byte Count: " + numberWithCommas(bytecount));
	console.log("Chunks Written: " + numberWithCommas(chunkwrite));
	console.log("Bytes Written: " + numberWithCommas(bytewrite));
	fs.writeFileSync('cache.json',JSON.stringify(newcache));
}).fail(function(err) {
	console.log("FAILED: " + err);
}).done();