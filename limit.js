var Q = require('q');

function limit(count, dm) {
	var waiting = [];

	function runnext() {
		if (waiting.length === 0) {
			return;
		}
		var next = waiting.shift();
		var deferred = next[0];
		var cb = next[1];
		cb(function(data) {
			//console.log("Rate limit done: " + dm + " " + waiting.length)
			count++;
			if (waiting.length !== 0) {
				count--;
				process.nextTick(runnext);
			}
			deferred.resolve(data);
		});
	}

	return function(debugmessage) {
		return {
			then: function(cb) {
				var deferred = Q.defer();
				waiting.push([deferred, cb]);
				if (count > 0) {
					count--;
					process.nextTick(runnext);
				} else {
					//console.log("Rate limiting: " + dm + ":" + debugmessage + " " + waiting.length)
				}
				return deferred.promise;
			}
		};
	};
}

module.exports = {
	limit: limit
};
