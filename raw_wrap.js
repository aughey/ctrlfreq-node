var Q = require('q');
var kv = require('./kv');
var hash = require('./hash');
var path = require('path');

function create(shadb, raw) {
	return {
		save: function(buffer, precomputedkey, debug) {
			if (precomputedkey) {
				var digest = precomputedkey;
			} else {
				var digest = hash.hash(buffer);
			}
			return raw.save_raw(digest,buffer).then(function(rawkey) {
				return digest;
			})
		},
	}
}

module.exports = {
	wrap: function(dir, raw) {
		return kv.create(path.join(dir, 'shadb')).then(function(shadb) {
			return create(shadb, raw);
		})
	}
}