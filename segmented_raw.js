var Q = require('q');
var path = require('path');
var limit = require('./limit');
var fs = require('fs-ext');

function create(dir) {
	var l = limit.limit(1);

	return Q.fcall(function() {
		var file = null;
		var curpos = 0;
		var max_size = 2000000000;

		function open(size) {
			var deferred = Q.defer();

			if (file) {
				if (curpos + size < max_size) {
					deferred.resolve(file);
					return deferred.promise;
				} else {
					// we're done with this one, open the next
					file.end(function() {
						open_next(size, deferred);
					});
				}
			} else {
				open_next(size, deferred);
			}

			return deferred.promise;
		}

		function open_next(size, deferred) {
			var index = 0;
			while (true) {
				var fullpath = path.join(dir, 'data' + index + '.raw');
				var stat = null;
				try {
					stat = fs.statSync(fullpath);
				} catch(e) {
					stat = { length: 0 };
				}
				if (stat.length + size < max_size) {
					console.log("Opening " + fullpath);
					file = fs.createWriteStream(fullpath, {
						flags: 'a'
					});
					file.once('open', function(fd) {
						console.log("opened");
						fs.fstat(fd, function(err, stats) {
							curpos = stats.size;
							deferred.resolve(file);
						})
					});
					return;
				}
				index += 1;
			}
		}

		return {
			save_raw: function(key, buffer) {
				return l().then(function(done) {
					open(buffer.length).then(function(stream) {
						var pos = curpos;
						var header = key.toString() + "\n";
						curpos += header.length + buffer.length;
						stream.write(header);
						stream.write(buffer, function() {
							done(pos.toString());
						});
					}).done(); // (this done is correct because of how limit works)
				});
			},
			close: function() {
				if (file) {
					return Q.ninvoke(file, 'end');
				} else {
					return Q.fcall(function() {

					});
				}
			}
		};
	});
}

module.exports = {
	create: create
};