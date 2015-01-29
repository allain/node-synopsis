module.exports = Synopsis;

var LRU = require('node-lru');
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

  var cache = new LRU({
    expires: 5 * 60 * 1000,
    capacity: 100
  });

  EventEmitter.call(this);

  assert(typeof options === 'object');

  var patcher = options.patcher;
  var differ = options.differ;

  var granularity = options.granularity || 5;

  var count;
  var tail;

  var deltaCache = this.deltaCache = {};

  var store = decorateStore(options.store || require('./stores/memory')());
  var innerGet = store.get;
  store.get = function (key, cb) {
    var cached = cache.get(key);
    if (cached) return cb(null, cached);

    innerGet(key, function (err, value) {
      cache.set(key, value);
      cb(err, value);
    });
  };

  function snapshot(index, cb) {
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

    delta(0, index, function (err, d) {
      if (err) return cb(err);

      setImmediate(function () {
        patcher(options.start, d, cb);
      });
    });
  }

  var patchQueue = async.queue(function (task, cb) {
    var delta = task.patch;

    async.series([
      function (cb) {
        store.set((count + 1) + '-1', delta, cb);
   },
      function (cb) {
        updateAggregates(delta, count + 1, cb);
      }
    ], function (err) {
      if (err) return cb(err);

      store.set('count', ++count, function (err) {
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
    snapshot(function (err, s) {
      if (err) return cb(err);

      patcher(s, patch, cb);
    });
  }

  function updateAggregates(delta, count, cb) {
    var scale = granularity;

    if (scale > count || count % scale !== 0) {
      return setImmediate(cb);
    }

    async.waterfall([
      function (cb) {
        snapshot(count - 1, cb);
   },
      function (prevSnapshot, cb) {
        patcher(prevSnapshot, delta, cb);
   }
  ], function (err, after) {
      if (err) return cb(err);

      var scales = [];

      while (scale <= count && count % scale === 0) {
        scales.push(scale);
        scale *= granularity;
      }

      var aggregates = {};
      async.eachSeries(scales, function (scale, cb) {
        snapshot(count - scale, function (err, before) {
          if (err) return cb(err);
          differ(before, after, function (err, diff) {
            if (err) return cb(err);

            aggregates[count + '-' + scale] = diff;
            cb();
          });
        });
      }, function (err) {
        if (err) return cb(err);

        store.setAll(aggregates, cb);
      });
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

  var collectDeltas = function (idx1, idx2, cb) {
    if (idx2 > count) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order: ' + idx1 + ' > ' + idx2));

    if (idx1 === idx2) return cb(null, []);

    var keys = computeIntervalKeys(idx1, idx2);

    store.getAll(keys, function (err, deltaMap) {
      if (err) return cb(err);

      cb(null, Object.keys(deltaMap).map(function (key) {
        return deltaMap[key];
      }));
    });
  };

  var delta = function (idx1, idx2, cb) {
    if (typeof cb === 'undefined') {
      cb = idx2;
      idx2 = count;
    }

    async.parallel({
      deltas: function (cb) {
        collectDeltas(idx1, idx2, cb);
      },
      start: function (cb) {
        snapshot(idx1, cb);
      }
    }, function (err, result) {
      async.reduce(result.deltas, result.start, patcher, function (err, end) {
        differ(result.start, end, cb);
      });
    });
  };

  function size(cb) {
    if (count || count === 0) return cb(null, count);

    store.get('count', function (err, c) {
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
      delta(startIndex, count, function (err, patch) {
        stream.push([patch, count]);
        streamPatches();
      });
    }

    function streamPatches() {
      var lastIndex = count;
      // Naive push everything approach, it should support pausing then resuming,
      // in the event that the stream is too slow to keep up.
      // It should compute the best delta when that happens

      var flowing = false;
      var pendingPatches = [];

      self.on('patched', function (patch) {
        if (flowing) {
          flowing = stream.push([patch, count]);
        } else {
          pendingPatches.push(count);
        }
      });

      stream._read = function () {
        flowing = true;
        if (pendingPatches.length) {
          var last = pendingPatches[pendingPatches.length - 1];
          self.delta(pendingPatches[0] - 1, count, function (err, delta) {
            flowing = stream.push([delta, last]);
            if (flowing) {
              pendingPatches = [];
            }
          });
        }
      };

      stream._write = function (chunk, encoding, next) {
        self.patch(chunk, function (err) {
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

  size(function (err) {
    if (err) {
      //TODO: Expose this failure to users
      return console.error(err);
    }

    store.get('head', function (err, storedHead) {
      head = storedHead || 0;
      process.nextTick(function () {
        self.emit('ready');
      });
    });

  });

  this.snapshot = snapshot;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
  this.size = size;
  this.createStream = createStream;
}

function decorateStore(store) {
  if (!store.setAll) {
    store.setAll = function (map, cb) {
      async.eachSeries(Object.keys(map), function (key, cb) {
        store.set(key, map[key], cb);
      }, cb);
    };
  }

  if (!store.getAll) {
    store.getAll = function (keys, cb) {
      var result = {};
      async.eachSeries(keys, function (key, cb) {
        store.get(key, function (err, val) {
          if (err) return cb(err);

          result[key] = val;

          cb();
        });
      }, function (err) {
        cb(err, result);
      });
    };
  }

  return store;
}