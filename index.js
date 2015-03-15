module.exports = Synopsis;

var assert = require('assert');
var async = require('async');
var values = require('amp-values');
var Duplex = require('stream').Duplex;
var Promise = require('bluebird');

var EventEmitter = require('events').EventEmitter;

var decorateStore = require('./stores/decorator.js');

var inherits = require('util').inherits;

inherits(Synopsis, EventEmitter);

function Synopsis(options) {
  if (!(this instanceof Synopsis)) {
    return new Synopsis(options);
  }

  var self = this;

  EventEmitter.call(this);

  var head;
  var tail;
  var emptyPatch;
  var emptyPatchJson;

  var patcher = options.patcher;
  var differ = options.differ;
  var granularity = options.granularity || 5;
  var store = decorateStore(options.store || require('./stores/memory')());

  // exists so that only 1 change operation can be done at a time
  var changeQueue = async.queue(function (task, cb) {
    return task(cb);
  }, 1);

  // Compute what the empty patch looks like
  differ(options.start, options.start, function (err, patch) {
    emptyPatch = patch;
    emptyPatchJson = JSON.stringify(patch);
  });

  function getInterval(intervalKey, cb) {
    store.get(intervalKey, function (err, p) {
      if (err) return cb(err);

      cb(null, p || emptyPatch);
    });
  }

  function getIntervals(intervalKeys, cb) {
    store.getAll(intervalKeys, function(err, map) {
      if (err) return cb(err);

      Object.keys(map).forEach(function(key) {
        map[key] = map[key] || emptyPatch;
      });

      return cb(null, map);
    });
  }

  var setInterval = function(intervalKey, value, cb) {
    if (JSON.stringify(value) === emptyPatchJson) {
      store.remove(intervalKey, cb);
    } else {
      store.set(intervalKey, value, cb);
    }
  };

  function setIntervals(intervalsMap, cb) {
    Object.keys(intervalsMap).forEach(function(interval) {
      var patch = intervalsMap[interval];
      if (JSON.stringify(patch) === emptyPatchJson) {
        intervalsMap[interval] = undefined;
      }
    });

    store.putAll(intervalsMap, cb);
  }

  function snapshot(index, cb) {
    if (!cb) {
      cb = index;
      index = head;
    } else if (typeof index === 'undefined') {
      index = head;
    }

    if (index === 0) {
      return cb(null, options.start);
    }

    delta(0, index, function (err, d) {
      if (err) return cb(err);

      patcher(options.start, d, cb);
    });
  }

  function patch(delta, cb) {
    testNewPatch(delta, function(err) {
      if (err) return cb(err);

      changeQueue.push(function(cb) {
        async.series([
          function(cb) {
            setInterval((head + 1) + '-1', delta, cb);
          },
          function(cb) {
            computeAggregates(delta, head + 1, cb);
          }
        ], function (err) {
          if (err) return cb(err);

          store.set('head', ++head, function (err) {
            self.emit('patched', delta);
            cb();
          });
        });
      }, cb);
    });
  }

  function testNewPatch(patch, cb) {
    snapshot(function (err, s) {
      if (err) return cb(err);

      patcher(s, patch, cb);
    });
  }

  var computeAggregates = function(delta, index, cb) {
    var scale = granularity;

    if (scale > index || index % scale !== 0) {
      return setImmediate(cb);
    }

    async.waterfall([
      function(cb) {
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

      var intervalChanges = {};

      async.eachSeries(scales, function (scale, cb) {
        snapshot(index - scale, function (err, before) {
          if (err) return cb(err);

          differ(before, after, function (err, diff) {
            if (err) return cb(err);

            intervalChanges[index + '-' + scale] = diff;
            cb();
          });
        });
      }, function(err) {
        if (err) return cb(err);

        setIntervals(intervalChanges, cb);
      });
    });
  };

  var updateAggregates = function(index, cb) {
    getInterval(index + '-1', function (err, patch) {
      computeAggregates(patch, index, cb);
    });
  };

  function delta(idx1, idx2, cb) {
    if (typeof cb === 'undefined') {
      cb = idx2;
      idx2 = head;
    }

    if (idx1 === idx2) {
      return cb(null, emptyPatch);
    }

    assert(typeof cb === 'function');

    async.parallel({
      deltas: function(cb) {
        collectDeltas(idx1, idx2, cb);
      },
      start: function(cb) {
        snapshot(idx1, cb);
      }
    }, function (err, result) {
      if(err) return cb(err);

      async.reduce(result.deltas, result.start, patcher, function (err, end) {
        if (err) return cb(err);

        differ(result.start, end, cb);
      });
    });
  }

  var collectDeltas = function(idx1, idx2, cb) {
    if (idx2 > head) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order: ' + idx1 + ' > ' + idx2));

    if (idx1 === idx2) return cb(null, []);

    getIntervals(computeIntervalKeys(idx1, idx2), function(err, intervals) {
      if (err) return cb(err);

      cb(null, values(intervals));
    });
  };

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
    } else if (startIndex < tail) {
      delta(tail, head, function (err, patch) {
        stream.push([patch, head, 'reset']);
        streamPatches();
      });
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

    changeQueue.push(function(cb) {
      tail ++;

      delta(tail - 1, tail + 1, function (err, delta) {
        async.series([
          function(cb) {
            var change = {};
            change[(tail + 1) + '-1'] = delta;
            change[tail + '-1'] = undefined;
            store.putAll(change, cb);
          },
          function(cb) {
            updateAggregates(tail, cb);
          },
          function(cb) {
            updateAggregates(tail + 1, cb);
          }
        ], function(err) {
          if (err) return cb(err);
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
    }, cb);
  }

  function stats(cb) {
    store.getAll(['head', 'tail'], function(err, s) {
      if (err) return cb(err);

      s.head = s.head || 0;
      s.tail = s.tail || 0;

      cb(null, s);
    });
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
