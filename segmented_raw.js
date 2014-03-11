var Q = require('q');
var path = require('path');
var limit = require('./limit');
var fs = require('fs-ext');

function create(dir) {
	var l = limit.limit(1);

	return Q.fcall(function() {
		var file = null;
		var curpos = null;

		function open() {
			var deferred = Q.defer();
			if (!file) {
				var fullpath = path.join(dir, 'data.raw');
				console.log("Opening " + fullpath);
				file = fs.createWriteStream(fullpath, {
					flags: 'a'
				});
				file.once('open', function(fd) {
					console.log("opened");
					fs.fstat(fd,function(err,stats) {
						deferred.resolve(file);
						curpos = stats.size;
					})
				});
			} else {
				deferred.resolve(file);
			}
			return deferred.promise;
		}

		return {
			save_raw: function(key, buffer) {
				return l().then(function(done) {
					open().then(function(stream) {
						var pos = curpos;
						curpos += buffer.length;

						stream.write(buffer,function() {
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