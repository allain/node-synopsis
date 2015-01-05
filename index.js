module.exports = Synopsis;

var assert = require('assert');
var async = require('async');

var EventEmitter = require('events').EventEmitter;

var inherits = require('util').inherits;

var noop = function() {};


inherits(Synopsis, EventEmitter);

function Synopsis(options) {
  if (!(this instanceof Synopsis)) {
    return new Synopsis(options);
  }

  var self = this;

  EventEmitter.call(this);

  assert(typeof options === 'object');

  var granularity = options.granularity || 5;

  var count = 0;

  var deltaCache = this.deltaCache = {};
  var differ = options.differ;
  var patcher = options.patcher;

  var store = options.store || (function() {
    var cache = [];
    return {
      get: function(key, cb) {
        return cb(null, cache[key]);
      },
      set: function(key, value, cb) {
        cache[key] = value;
        cb();
      }
    };
  })();

  function sum(index, cb) {
    if (typeof cb === 'undefined') {
      cb = index;
      index = count;
    }

    if (typeof index === 'undefined') {
      index = count;
    }

    if (index === 0) {
      return cb(null, options.start);
    }

    delta(0, index, function(err, d) {
      if (err) return cb(err);

      cb(null, patcher(options.start, d));
    });
  }

  function patch(delta, cb) {
    cb = cb || noop;

    store.set(++ count + '-1', delta, function(err) {
      if (err) return cb(err);

      updateAggregates(cb);
    });
  }

  function updateAggregates(cb) {
    var scale = granularity;

    async.whilst(function() {
      return scale <= count && count % scale === 0;
    }, function(next) {
      async.parallel({
        before: function(cb) {
          return sum(count - scale, cb);
        },
        after: function(cb) {
          return sum(count, cb);
        }
      }, function(err, sums) {
        store.set(count + '-' + scale, differ(sums.before, sums.after), function(err) {
          scale *= granularity;
          next();
        });
      });
    }, function(err) {
      cb();
    });
  }

  function collectDeltas(idx1, idx2, cb) {
    var result = [];

    if (idx2 > count) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order'));

    if (idx1 === idx2) return cb(null, []);

    var idx = idx2;

    async.whilst(function() {
      return idx > idx1;
    }, function(next) {
      if (idx === 0) {
        idx --;
        return next();
      } else if (idx % granularity !== 0) {
        store.get(idx -- + '-1', function(err, delta) {
          result.push(delta);
          return next();
        });
        return;
      }

      var deltaSize = idx2 - idx1;
      var deltaScale = Math.pow(granularity, Math.floor(Math.log(deltaSize)/Math.log(granularity)));

      var cached;

      async.doWhilst(function(next) {
        if (idx % deltaScale === 0) {
          store.get(idx + '-' + deltaScale, function(err, c) {
            if (c) {
              cached = c;
            } else {
              deltaScale /= granularity;
            }
            next();
          });
        } else {
          deltaScale /= granularity;
          next();
        }
      }, function() {
        return !cached && deltaScale > 1;
      }, function(err) {
        if (err) return next(err);
        if (cached) {
          result.push(cached);
          idx -= deltaScale;
          next();
        } else {
          store.get(idx -- + '-1', function(err, delta) {
            result.push(delta);
            next();
          });
        }
      });
    }, function(err) {
      cb(err, result.reverse());
    });
  }

  function delta(idx1, idx2, cb) {
    if (typeof cb === 'undefined') {
      cb = idx2;
      idx2 = count;
    }

    collectDeltas(idx1, idx2, function(err, deltas) {
      if (err) return cb(err);

      sum(idx1, function(err, start) {
        if (err) return cb(err);

        var result = start;

        deltas.forEach(function(delta) {
          result = patcher(result, delta);
        });

        cb(null, differ(start, result));
      });
    });
  }

  store.get('count', function(err, c) {
    count = c || 0;
    process.nextTick(function() {
      self.emit('ready');
    });
  });


  this.sum = sum;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
}
