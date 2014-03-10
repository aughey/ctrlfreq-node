var levelup = require('levelup');
var Q = require('q');

function noop() {}

module.exports = {
	create: function(path) {
		return Q.nfcall(levelup, path).then(function(db) {
			return {
				has: function(key, cb) {
					var iterator = db.db.iterator({
						start: key,
						values: false
					});
					return Q.ninvoke(iterator,'next').then(function(ikey) {
						iterator.end(noop);
						if (!ikey) {
							return false;
						}
						if (ikey == key) {
							return true;
						} else {
							return false;
						}
					});
				},
				put: function(key,value,cb) {
					db.put(key, value, cb);
				},
				get: function(key,cb) {
					db.get(key,cb);
				},
				close: function() {
					return Q.ninvoke(db, 'close');
				}
			}
		});
	}
}