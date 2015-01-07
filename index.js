module.exports = Synopsis;

var assert = require('assert');
var async = require('async');

var EventEmitter = require('events').EventEmitter;

var inherits = require('util').inherits;

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

      setImmediate(function() {
        cb(null, patcher(options.start, d));
      });
    });
  }

  function patch(delta, cb) {
    testNewPatch(delta, applyPatch);

    function applyPatch(err) {
      if (err) {
        return cb(err);
      }

      // TODO: make these two set operations atomic
      store.set(++ count + '-1', delta, function(err) {
        if (err) return cb(err);

        store.set('count', count, function(err) {
          if (err) return cb(err);

          updateAggregates(delta, function(err) {
            if (err) return cb(err);

            self.emit('patched', delta);

            cb();
          });
        });
      });
    }
  }

  function testNewPatch(patch, cb) {
    sum(function(err, s) {
      if (err) return cb(err);

      try {
        patcher(s, patch);
      } catch(e) {
        return cb(new Error('Invalid Patch ' + JSON.stringify(patch) + " to " + JSON.stringify(s)));
      }
      cb();
    });
  }

  function updateAggregates(delta, cb) {
    var scale = granularity;

    if (scale > count || count % scale !== 0) {
      return setImmediate(cb);
    }

    var after;
    sum(count - 1, function(err, prevSum) {
      if (err) return cb(err);

      after = patcher(prevSum, delta);

      async.whilst(function() {
        return scale <= count && count % scale === 0;
      }, function(next) {
        sum(count - scale, function(err, before) {
          if (err) return next(err);

          store.set(count + '-' + scale, differ(before, after), function(err) {
            if (err) return next(err);

            scale *= granularity;
            next();
          });
        });
      }, cb);
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

      var deltaSize = idx - idx1;
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

  function size(cb) {
    if (count || count === 0) return cb(null, count);

    store.get('count', function(err, c) {
      if (err) return cb(err);

      count = c || 0;

      cb(null, count);
    });
  }

  this.sum = sum;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
  this.size = size;
}
