var fs = require('fs');
var _ = require('underscore');
var Q = require('q');

var cachecount = 0;

function init(cachefile, chain) {
	var filecache = {};
	try {
		filecache = JSON.parse(fs.readFileSync(cachefile));
		console.log("Loaded filecache");
	} catch (e) {
		filecache = {};
	}
	var newcache = {};

	function processfile_cache(fullpath, name, stat, store, emitter) {
		var cache = filecache[fullpath];
		if (cache) {
			//console.log(cache.mtime + " " + stat.mtime.toISOString())
			//console.log(JSON.stringify(stat));

			if (_.isEqual(stat, cache.stat)) {
				cachecount++;
				newcache[fullpath] = cache;
				return Q.fcall(function() {
					return cache.chunks;
				});
			}
		}
		return chain(fullpath, name, stat, store, emitter).then(function(chunks) {
			newcache[fullpath] = {
				stat: stat,
				chunks: chunks
			};
			return chunks;
		});
	}

	return {
		processfile: processfile_cache,
		saveCache: function() {
			fs.writeFileSync(cachefile, JSON.stringify(newcache));
		}
	}
}

module.exports = {
	init: init
}