var ctrlfreq = require('./ctrlfreq');
var leveldbstore = require('./leveldb_store');
var nullstore = require('./null_store');
var ss = require('./segmented_store');
var _ = require('underscore')
var Q = require('q');
var processfile_full = require("./processfile_full")
var processfile_cache = require("./processfile_cache")

var levelup = require('levelup');

var dbpath = "./db/segmented";

var args = process.argv;
args.shift();
args.shift();

var storetouse = ss;
var processfile_cache = processfile_cache.init("cache.json",processfile_full.processfile);
var processfile = processfile_cache

var backups = [];
storetouse.create(dbpath).then(function(store) {
	_.each(args, function(dir) {
		console.log("Backing up " + dir);
		var backup = ctrlfreq.backup(dir, store, processfile.processfile);
		backup.emitter.on('file', function(file) {
			console.log("Storing: " + file);
		});
		backups.push(backup.promise);
	});
	return Q.all(backups).then(function() {
		console.log("All backups done");
		return store.close();
	});
}).then(function() {
	console.log("db closed");
	if(processfile_cache) {
		processfile_cache.saveCache();
	}
}).done();
