var Q = require('q');
var kv = require('./kv');
var hash = require('./hash');
var path = require('path');

function create(shadb, raw) {
	return {
		save: function(buffer, precomputedkey, debug) {
			var digest;
			if (precomputedkey) {
				digest = precomputedkey;
			} else {
				digest = hash.hash(buffer);
			}
			return raw.save_raw(digest,buffer).then(function(rawkey) {
				console.log("rawkey = " + rawkey);
				return digest;
			});
		},
		close: function() {
			return shadb.close().then(function() {
					return raw.close();
			});
		}
	};
}

module.exports = {
	wrap: function(dir, raw) {
		return kv.create(path.join(dir, 'shadb')).then(function(shadb) {
			return create(shadb, raw);
		});
	}
};
