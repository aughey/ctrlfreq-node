var fs = require('fs');
var limit = require('./limit').limit;
var zlib = require('zlib');

var filelimit = limit(10, "filelimit");
var readlimit = limit(100, "readlimit");

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
	};
}

function processfile_full(fullpath, name, stat, store, emitter) {
	return filelimit(fullpath).then(function(filedone) {
		emitter.emit('file', fullpath);
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
					if (outstanding === 0) {
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
						if (bytesread === 0) {
							fs.close(fd);
							bufferdone();
							checkdone();
							return;
						}
						emitter.emit("filechunk", fullpath, bytesread, stat.size);
						buffer = buffer.slice(0, bytesread);
						var unlog = bytecheck(buffer.length);
						var index = chunks.length;
						chunks.push(null);
						outstanding += 1;

						zlib.deflate(buffer, function(err, compressed_buffer) {
							store.save(compressed_buffer, null, fullpath).then(function(uuid) {
								unlog();
								chunks[index] = uuid;
								bufferdone();
								checkdone();
							}).done();
							readnext();
						})

					});
				});

			}

			readnext();
		});
	});
}

module.exports = {
	processfile: processfile_full
};