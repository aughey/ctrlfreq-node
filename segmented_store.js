var raw = require('./segmented_raw');
var wrap = require('./raw_wrap');

module.exports = {
	create: function(dir) {
		return raw.create(dir).then(function(rawstore) {
			return wrap.wrap(dir,rawstore);
		});
	}
};
