var benchmark = require('bench-csv');
var Synopsis = require('..');

var async = require('async');
var microtime = require('microtime');

benchmark(function () {
    return {
      time: microtime.now()
    };
  },
  function (n, cb) {
    var syn = new Synopsis({
      differ: function (before, after) {
        return after - before;
      },
      patcher: function (patch, state) {
        return state + patch;
      }
    });

    syn.on('ready', function () {
      var index = 0;
      async.whilst(function () {
        return index <= n;
      }, function (cb) {
        syn.patch(index++, cb);
      }, function () {
        cb();
      });
    });
  });