module.exports = Synopsis;

var assert = require('assert');

function Synopsis(options) {
  options = options || {};

  var granularity = options.granularity || 5;
  var count = options.count || 0;


  var updates = [];
  var deltaCache = this.deltaCache = {};
  var differ = options.differ;
  var patcher = options.patcher;

  var store = options.store || (function() {
    var cache = [];
    return {
      get: function(key) {
        var parts = key.split('-');
        return cache[parts[0]] && cache[parts[0]][parts[1]];
      },
      set: function(key, value) {
        var parts = key.split('-');
        cache[parts[0]] = cache[parts[0]] || {};
        cache[parts[0]][parts[1]] = value;
      }
    };
  })();

  this.sum = function(index) {
    if (typeof index === 'undefined') {
      index = count;
    }

    if (index === 0) {
      return options.start;
    }

    var delta = this.delta(0, index);

    return patcher(options.start, delta);
  };

  this.patch = function(delta) {
    updates.push([delta, ++count]);

    var scale = granularity;
    while (scale <= count && count % scale === 0) {
      store.set(count + '-' + scale, differ(this.sum(count - scale), this.sum(count)));
      scale *= granularity;
    }

    return count;
  };

  this.collectDeltas = function(idx1, idx2) {
    var result = [];

    assert(idx1 <= idx2, 'delta in incorrect order');

    if (idx1 === idx2) return result;

    var idx = idx2;
    while (idx > idx1) {
      if (idx % granularity !== 0) {
        result.push(updates[--idx][0]);
        continue;
      }

      var deltaSize = idx2 - idx1;
      var deltaScale = Math.pow(granularity, Math.floor(Math.log(deltaSize)/Math.log(granularity)));

      var cached;
      do {
        //TODO: Support this result being a Promise
        cached = (idx % deltaScale === 0) && store.get(idx + '-' + deltaScale);
        if (cached) break;

        deltaScale /= granularity;
      } while (!cached && deltaScale > 1);

      if (cached) {
        result.push(cached);
        idx -= deltaScale;
      } else {
        result.push(updates[--idx][0]);
      }
    }

    return result.reverse();
  };

  this.delta = function(idx1, idx2) {
    if (typeof idx2 === 'undefined') {
      idx2 = count;
    }
    var deltas = this.collectDeltas(idx1, idx2);
    var start = this.sum(idx1);
    var result = start;
    deltas.forEach(function(delta) {
      result = patcher(result, delta);
    });

    return differ(start, result);
  };
}
