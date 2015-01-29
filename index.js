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

  var patcher = options.patcher;
  var differ = options.differ;

  var granularity = options.granularity || 5;

  var head;
  var tail;

  var emptyPatch;
  var emptyPatchJson;

  differ(options.start, options.start, function (err, patch) {
    emptyPatch = patch;
    emptyPatchJson = JSON.stringify(patch);
  });

  var deltaCache = this.deltaCache = {};

  var store = decorateStore(options.store || require('./stores/memory')());

  function getInterval(intervalKey, cb) {
    store.get(intervalKey, function (err, p) {
      if (err) return cb(err);

      if (p) {
        cb(null, p);
      } else {
        cb(null, emptyPatch);
      }
    });
  }

  function setInterval(intervalKey, value, cb) {
    if (JSON.stringify(value) === emptyPatchJson) {
      store.remove(intervalKey, cb);
    } else {
      store.set(intervalKey, value, cb);
    }
  }

  function snapshot(index, cb) {
    if (typeof cb === 'undefined') {
      cb = index;
      index = head;
    }

    if (typeof index === 'undefined') {
      index = head;
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

  //exists so that only 1 scary operation can be done at a time
  var workQueue = async.queue(function (task, cb) {
    var patch = task.patch;

    if (patch) {
      async.series([
        function (cb) {
          setInterval((head + 1) + '-1', patch, cb);
        },
        function (cb) {
          computeAggregates(patch, head + 1, cb);
        }
      ], function (err) {
        if (err) return cb(err);

        store.set('head', ++head, function (err) {
          self.emit('patched', patch);
          cb();
        });
      });
    } else if (task.compact) {
      tail++;

      delta(tail - 1, tail + 1, function (err, delta) {
        store.set((tail + 1) + '-1', delta, function (err) {
          store.remove(tail + '-1', function (err) {
            updateAggregates(tail, function (err) {
              updateAggregates(tail + 1, function (err) {
                var indexes = {};
                var scale = granularity;
                var nextIndex;

                while (scale <= head) {
                  nextIndex = Math.ceil((tail + 1) / scale) * scale;
                  if (nextIndex > head) break;
                  indexes[nextIndex] = true;
                  scale *= granularity;
                }

                async.eachSeries(Object.keys(indexes), updateAggregates, function (err) {
                  store.set('tail', tail, cb);
                });
              });
            });
          });
        });
      });
    }
  }, 1);

  function patch(delta, cb) {
    testNewPatch(delta, applyPatch);

    function applyPatch(err) {
      if (err) return cb(err);

      workQueue.push({
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

  function computeAggregates(delta, index, cb) {
    var scale = granularity;

    if (scale > index || index % scale !== 0) {
      return setImmediate(cb);
    }

    async.waterfall([
      function (cb) {
        snapshot(index - 1, cb);
      },
      function (prevSnapshot, cb) {
        patcher(prevSnapshot, delta, cb);
      }
    ], function (err, after) {
      if (err) return cb(err);

      var scales = [];

      while (scale <= index && index % scale === 0) {
        scales.push(scale);
        scale *= granularity;
      }

      var aggregates = {};
      async.eachSeries(scales, function (scale, cb) {
        snapshot(index - scale, function (err, before) {
          if (err) return cb(err);

          differ(before, after, function (err, diff) {
            if (err) return cb(err);

            setInterval(index + '-' + scale, diff, cb);
          });
        });
      }, cb);
    });
  }

  function updateAggregates(index, cb) {
    getInterval(index + '-1', function (err, patch) {
      computeAggregates(patch, index, cb);
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
    if (idx2 > head) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order: ' + idx1 + ' > ' + idx2));

    if (idx1 === idx2) return cb(null, []);

    var keys = computeIntervalKeys(idx1, idx2);

    async.reduce(keys, [], function (deltas, key, cb) {
      getInterval(key, function (err, delta) {
        deltas.push(delta);
        cb(null, deltas);
      });
    }, function (err, deltas) {
      cb(err, deltas);
    });
  };

  var delta = function (idx1, idx2, cb) {
    if (typeof cb === 'undefined') {
      cb = idx2;
      idx2 = head;
    }

    if (idx1 === idx2) {
      return cb(null, emptyPatch);
    }

    assert(typeof cb === 'function');

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

  function createStream(startIndex, cb) {
    if (typeof cb === 'undefined') {
      cb = startIndex;
      startIndex = 0;
    }

    var stream = new Duplex({
      objectMode: true
    });

    if (startIndex === head) {
      streamPatches();
    } else {
      delta(startIndex, head, function (err, patch) {
        stream.push([patch, head]);
        streamPatches();
      });
    }

    function streamPatches() {
      var lastIndex = head;
      // Naive push everything approach, it should support pausing then resuming,
      // in the event that the stream is too slow to keep up.
      // It should compute the best delta when that happens

      var flowing = false;
      var pendingPatches = [];

      self.on('patched', function (patch) {
        if (flowing) {
          flowing = stream.push([patch, head]);
        } else {
          pendingPatches.push(head);
        }
      });

      stream._read = function () {
        flowing = true;
        if (pendingPatches.length) {
          var last = pendingPatches[pendingPatches.length - 1];
          self.delta(pendingPatches[0] - 1, head, function (err, delta) {
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

  function compact(cb) {
    if (tail + 1 >= head) {
      return cb();
    }

    workQueue.push({
      compact: 1
    }, cb);
  }

  function stats(cb) {
    async.parallel({
      head: function (cb) {
        store.get('head', function (err, h) {
          cb(err, h || 0);
        });
      },
      tail: function (cb) {
        store.get('tail', function (err, t) {
          cb(err, t || 0);
        });
      }
    }, cb);
  }

  stats(function (err, s) {
    head = s.head;
    tail = s.tail;
    process.nextTick(function () {
      self.emit('ready');
    });
  });

  this.snapshot = snapshot;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
  this.computeIntervalKeys = computeIntervalKeys;
  this.stats = stats;
  this.createStream = createStream;
  this.compact = compact;
}

function decorateStore(store) {
  if (!store.setAll) {
    store.setAll = function (map, cb) {
      async.eachSeries(Object.keys(map), function (key, cb) {
        store.set(key, map[key], cb);
      }, cb);
    };
  }

  return store;
}