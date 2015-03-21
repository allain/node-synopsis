var benchmark = require('bench-csv');
var Synopsis = require('..');

var microtime = require('microtime');
var async = require('async');

benchmark(function() {
  return {
    time: microtime.now()
  };
}, function(n, cb) {
  var values = [];

  var index = 0;

  async.whilst(function() {
    return index <= n;
  }, function(cb) {
    values.push(index++);
    setImmediate(cb);
  }, function() {
    cb();
  });
});