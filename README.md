#synposis

[![build status](https://secure.travis-ci.org/allain/node-synopsis.png)](http://travis-ci.org/allain/node-synopsis)

Synopsis is a tool computing the effective deltas two points in time when all you are given is incremental updates.

The approach I'm taking is to take a granularity N, and then merge N consecutive deltas together into deltaN.

Then I do the same for NxN, NxNxN, etc. until the granularity is greater than the number of deltas.

###Simple Counting Example:
``` js
// Uber simple Synopsis example that just calculates a running
// total of a sum of integers with granularity of 5
var async = require('async');
var _ = require('lodash');

var s = new Synopsis({
  start: 0,
  granularity: 5,
  patcher: function(prev, patch) {
    return prev + patch;
  },
  differ: function(before, after) {
    return after - before;
  }
});

async.eachSeries(_.range(1, 1001), function(n, cb) {
  s.patch(n, cb);
}, function(err) {
  s.sum(function(err, s) {
    console.log(s); // Outputs 1000
  });

  s.sum(500, function(err, s) {
    console.log(s);  // Outputs 500
  });

  s.delta(0, 10, function(err, d) {
    console.log(d); // Outputs 10
  });

  s.delta(5, 7, function(err, d) {
    console.log(d); // Outputs 2  
  });
});

```

### Configuration

Supports specifying a custom store implementation, defaults to memory store if none is provided.

By doing so, the state of the Synopsis instance can be retained across restarts and none of the delta merging will need to be done again.

#### example:
``` js
var s = new Synopsis({
  store: {
    get: function(key, callback) { ... },
    put: function(key, callback) { ... }
  }
});
```
