module.exports = Synopsis;

var assert = require('assert');
var async = require('neo-async');
var values = require('amp-values');
var mapIn = require('map-in');

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

  // exists so that only 1 change operation can be done at a time, includes patches and compactions.
  var changeQueue = async.queue(function(task, cb) {
    return task(cb);
  }, 1);

  // Compute what the empty patch looks like
  differ(options.start, options.start, function(err, patch) {
    emptyPatch = patch;
    emptyPatchJson = JSON.stringify(patch);
  });

  function getDelta(deltaKey, cb) {
    store.get(deltaKey, function(err, p) {
      if (err) return cb(err);

      cb(null, p || emptyPatch);
    });
  }

  function getDeltas(deltaKeys, cb) {
    store.getAll(deltaKeys, function(err, map) {
      if (err) return cb(err);

      cb(null, mapIn(map, function(patch) {
        return patch || emptyPatch;
      }));
    });
  }

  function storeDelta(deltaKey, value, cb) {
    if (JSON.stringify(value) === emptyPatchJson) {
      store.remove(deltaKey, cb);
    } else {
      store.set(deltaKey, value, cb);
    }
  }

  function storeDeltas(deltaMap, cb) {
    store.putAll(mapIn(deltaMap, function(patch) {
      if (JSON.stringify(patch) !== emptyPatchJson) {
        return patch;
      }
    }), cb);
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

    delta(0, index, function(err, d) {
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
            storeDelta((head + 1) + '-1', delta, cb);
          },
          function(cb) {
            computeAggregates(delta, head + 1, cb);
          }
        ], function(err) {
          if (err) return cb(err);

          store.set('head', ++head, function(err) {
            self.emit('patched', delta);
            cb();
          });
        });
      }, cb);
    });
  }

  function testNewPatch(patch, cb) {
    snapshot(function(err, s) {
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
      function(prevSnapshot, cb) {
        patcher(prevSnapshot, delta, cb);
      }
    ], function(err, after) {
      if (err) return cb(err);

      var scales = [];

      while (scale <= index && index % scale === 0) {
        scales.push(scale);
        scale *= granularity;
      }

      async.reduce(scales, {}, function(deltaChanges, scale, cb) {
        snapshot(index - scale, function(err, before) {
          if (err) return cb(err);

          differ(before, after, function(err, diff) {
            if (err) return cb(err);

            deltaChanges[index + '-' + scale] = diff;
            cb(null, deltaChanges);
          });
        });
      }, function(err, deltaChanges) {
        if (err) return cb(err);

        storeDeltas(deltaChanges, cb);
      });
    });
  };

  function updateAggregates(index, cb) {
    getDelta(index + '-1', function(err, patch) {
      computeAggregates(patch, index, cb);
    });
  }

  function delta(idx1, idx2, cb) {
    if (typeof cb === 'undefined') {
      cb = idx2;
      idx2 = head;
    }

    if (idx1 === idx2) {
      return cb(null, emptyPatch);
    }

    assert(typeof cb === 'function');

    collectDeltas(idx1, idx2, function(err, deltas) {
      if (err) return cb(err);

      snapshot(idx1, function(err, start) {
        if (err) return cb(err);

        async.reduce(deltas, start, patcher, function(err, end) {
          if (err) return cb(err);

          differ(start, end, cb);
        });
      });
    });
  }

  function collectDeltas(idx1, idx2, cb) {
    if (idx2 > head) return cb(new Error('index out of range: ' + idx2));
    if (idx1 > idx2) return cb(new Error('delta in incorrect order: ' + idx1 + ' > ' + idx2));

    if (idx1 === idx2) return cb(null, []);

    getDeltas(computeDeltaKeys(idx1, idx2), function(err, deltas) {
      if (err) return cb(err);

      cb(null, values(deltas));
    });
  }

  function computeDeltaKeys(idx1, idx2) {
    var idx = idx2;

    var keys = [];

    while (idx > idx1 && idx !== 0) {
      if (idx % granularity !== 0) {
        keys.push(idx-- + '-1');
        continue;
      }

      var deltaSize = idx - idx1;
      var deltaScale = Math.pow(granularity, Math.floor(Math.log(deltaSize) / Math.log(granularity)));

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

  function compact(cb) {
    if (tail + 1 >= head) {
      return cb();
    }

    changeQueue.push(function(cb) {
      tail++;

      delta(tail - 1, tail + 1, function(err, delta) {
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

          async.eachSeries(Object.keys(indexes), updateAggregates, function(err) {
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

  stats(function(err, s) {
    head = s.head;
    tail = s.tail;
    process.nextTick(function() {
      self.emit('ready');
    });
  });

  this.snapshot = snapshot;
  this.delta = delta;
  this.patch = patch;
  this.collectDeltas = collectDeltas;
  this.computeDeltaKeys = computeDeltaKeys;
  this.stats = stats;
  this.compact = compact;
}
