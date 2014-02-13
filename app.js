var ctrlfreq = require('./ctrlfreq');
var leveldbstore = require('./leveldb_store');
var _ = require('underscore')
var Q = require('q');

var levelup = require('levelup');

var dbpath = "./db/chunks";

var args = process.argv;
args.shift()
args.shift()

var backups = [];
leveldbstore.store(dbpath).then(function(store) {
	_.each(args, function(dir) {
		console.log("Backing up " + dir);
		var backup = ctrlfreq.backup(dir, store)
		backup.emitter.on('file', function(file) {
			console.log("Storing: " + file)
		});;
		backups.push(backup.promise);
	})
	return Q.all(backups).then(function() {
		console.log("All backups done");
		return store.close();
	});
}).then(function() {
	console.log("db closed");
}).done();