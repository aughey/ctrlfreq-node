//var fs = require('fs');
var Q = require('q');
var path = require('path');
var limit = require('./limit');
var fs = require('fs-ext');

function create(dir) {
	var l = limit.limit(1);

	return Q.fcall(function() {
		var file = null;

		function open() {
			var deferred = Q.defer();
			if(!file) {
				var fullpath = path.join(dir,'data.raw');
				console.log("Opening " + fullpath);
				file = fs.createWriteStream(fullpath, {flags: 'a'});
				file.once('open', function(fd) {
					console.log("opened");
					deferred.resolve(file);
				});
			} else {
				deferred.resolve(file);
			}
			return deferred.promise;
		}

		return {
			save_raw: function(key, buffer) {
				return l().then(function(done) {
					return open().then(function(stream) {
						console.log("writing");
						var pos = fs.seekSync(stream.fd,0,2);
						console.log(pos);
						return Q.ninvoke(stream,'write',buffer).then(function(a) {
							console.log("returning " + pos);
							done(pos.toString());
						});
					});
				});
			},
			close: function() {
				if(file) {
					return Q.ninvoke(file,'close',file);
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
