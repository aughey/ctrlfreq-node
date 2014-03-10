var Q = require('q');
var hash = require('./hash');

var obj = {
	save: function(buffer, precomputedkey, debug) {
		return Q.fcall(function() {
			if (precomputedkey) {
				var digest = precomputedkey;
			} else {
				var digest = hash.hash(buffer);
			}
			return digest;
		})
	},
	close: function() {
		return Q.fcall(function() {})
	}
};

function create() {
	return Q.fcall(function() {
		return obj
	});
}

module.exports = {
	create: create
}