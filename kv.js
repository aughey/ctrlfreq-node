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
				put: function(key,value) {
					return Q.ninvoke(db,'put',key,value);
				},
				get: function(key) {
					return Q.ninvoke(db,'get',key);
				},
				close: function() {
					return Q.ninvoke(db, 'close');
				}
			}
		});
	}
}
