var fs = require('fs');
var Q = require('q');
var path = require('path');

function create(dir) {
	return Q.fcall(function() {
		var file = null;

		function open() {
			if(file) {
				return Q.fcall(function() {

				});
			} else {
				var fullpath = path.join(dir,'data.raw');
				console.log("Opening " + fullpath);
				return Q.ninvoke(fs,'createWriteStream',fullpath).then(function(stream) {
					file = stream;
					return stream;
				});
			}
		}

		return {
			save_raw: function(key, buffer) {
				return open().then(function(file) {
					console.log("writing")
					return Q.ninvoke(file,'write',buffer).then(function(a) {
						console.log("wrote " + a);
					})
				})
			},
			close: function() {
				if(file) {
					return Q.ninvoke(fs,'close',file);
				} else {
					return Q.fcall(function() {

					})
				}
			}
		}
	});
}

module.exports = {
	create: create
};