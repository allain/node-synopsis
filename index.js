module.exports = Synopsis;

var assert = require('assert');
var async = require('async');
var values = require('amp-values');
var Duplex = require('stream').Duplex;

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

  var count;

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
      },
      setAll: function(map, cb) {
        var self = this;
        async.eachSeries(Object.keys(map), function(key, cb) {
          self.set(key, map[key], cb);
        }, cb);
      }
    };
  })();

  if (!store.setAll) {
    store.setAll = function(map, cb) {
      async.eachSeries(Object.keys(map), function(key, cb) {
        store.set(key, map[key], cb);
      }, cb);
    };
  }

  if (!store.getAll) {
    store.getAll = function(keys, cb) {
      var result = {};
      async.eachSeries(keys, function(key, cb) {
        store.get(key, function(err, val) {
          if (err) return cb(err);

          result[key] = val;

          cb();
        });
      }, function(err) {
        if (err) return cb(err);

        return cb(null, result);
      });
    };
  }

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

  var patchQueue = async.queue(function(task, cb) {
    var delta = task.patch;

    ++count;

    var storeDoc = {};
    storeDoc[count + '-1'] = delta;
    storeDoc.count = count;

    store.setAll(storeDoc, function(err) {
      if (err) return cb(err);

      updateAggregates(delta, function(err) {
        if (err) return cb(err);

        self.emit('patched', delta);

        cb();
      });
    });
  }, 1);

  function patch(delta, cb) {
    testNewPatch(delta, applyPatch);

    function applyPatch(err) {
      if (err) return cb(err);

      patchQueue.push({
        patch: delta
      }, cb);
    }
  }

  function testNewPatch(patch, cb) {
    sum(function(err, s) {
      if (err) return cb(err);

      try {
        patcher(s, patch);
      } catch (e) {
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

  function computeIntervalKeys(idx1, idx2) {
    var idx = idx2;

    var keys = [];

    while (idx > idx1 && idx !== 0) {
      if (idx % granularity !== 0) {
        keys.push(idx--+'-1');
        continue;
      }

      var deltaSize = idx - idx1;
      var deltaScale = Math.pow(granularity, Math.floor(Math.log(deltaSize) / Math.log(granularity)));
      var cached;

      do {
        if (idx % deltaScale === 0) {
          break;
        } else {
          deltaScale /= granularity;
        }
      } while (idx > 0 && deltaScale > 1);

      keys.push(idx + '-' + deltaScale);

      idx -= deltaScale;
    }

    return keys.reverse();
  }

  function collectDeltas(idx1, idx2, cb) {
    if (idx2 > count) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order: ' + idx1 + ' > ' + idx2));

    if (idx1 === idx2) return cb(null, []);

    var keys = computeIntervalKeys(idx1, idx2);

    store.getAll(keys, function(err, deltaMap) {
      if (err) return cb(err);

      cb(null, values(deltaMap));
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

  function createStream(startIndex, cb) {
    if (typeof cb === 'undefined') {
      cb = startIndex;
      startIndex = 0;
    }

    var stream = new Duplex({
      objectMode: true
    });

    if (startIndex === count) {
      streamPatches();
    } else {
      delta(startIndex, count, function(err, patch) {
        stream.push([patch, count]);
        streamPatches();
      });
    }

    function streamPatches() {
      var lastIndex = count;
      // Naive push everything approach, it should support pausing then resuming
      // It should compute the best delta when that happens

      var pendingDelta;

      self.on('patched', function(patch) {
        stream.push([patch, count]);
      });

      stream._read = function() {};

      stream._write = function(chunk, encoding, next) {
        self.patch(chunk, function(err) {
          if (err) {
            stream.push({
              error: 'patch failed',
              patch: chunk,
              cause: err.message
            });
          }

          next();
        });
      };

      cb(null, stream);
    }
  }

  size(function(err) {
    if (err) {
      //TODO: Expose this failure to users
      return console.error(err);
    }

    process.nextTick(function() {
      self.emit('ready');
    });
  });

  this.sum = sum;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
  this.size = size;
  this.createStream = createStream;
}