var assert = require('assert');
var jiff = require('jiff');

var Synopsis = require('../index.js');

describe('Synopsis', function() {
  var s;
  beforeEach(function() {
    // Uber simple Synopsis example that just calculates the diff
    s = new Synopsis({
      start: 0,
      granularity: 5,
      patcher: function(prev, patch) {
        return prev + patch;
      },
      differ: function(before, after) {
        return after - before;
      }
    });
  });

  it('is sane when empty', function() {
    assert(0 === s.sum(), 'initial  should be given start');
  });

  it('computes sum when patches are added', function() {
    s.patch(2);
    s.patch(3);
    assert(s.sum(), 5);
  });

  it('aggregates deltas', function() {
    var deltaCount = 5;

    for (var i=0; i < deltaCount; i++) {
      s.patch(1);
    }

    assert.equal(s.delta(0, deltaCount), deltaCount);
  });

  it('sum behaves as expected', function() {
    [1,2,3,4].forEach(s.patch);

    assert.equal(1, s.sum(1));
    assert.equal(3, s.sum(2));
    assert.equal(6, s.sum(3));
    assert.equal(10, s.sum(4));
    assert.equal(10, s.sum());
    assert.equal(s.sum(2) - s.sum(1), s.delta(1,2));
    assert.equal(s.sum(3) - s.sum(1), s.delta(1,3));
    assert.equal(s.sum(4) - s.sum(1), s.delta(1,4));
    assert.equal(s.sum(4) - s.sum(2), s.delta(2,4));
  });

  it('uses delta merging', function() {
    for (var i=0; i < 125; i++) {
      s.patch(1);
    }

    assert.equal(5, s.sum(5));
    assert.deepEqual([1,1,1,1], s.collectDeltas(0, 4));
    assert.deepEqual([5], s.collectDeltas(0, 5));
    assert.deepEqual([5,1], s.collectDeltas(0, 6));
    assert.deepEqual([5,5], s.collectDeltas(0, 10));
    assert.deepEqual([125], s.collectDeltas(0, 125));
    assert.deepEqual([25,25,25,25,5,5,5,5,1,1,1,1], s.collectDeltas(0, 124));
  });

  it('supports delta from any two points in time', function() {
    for (var i=0; i < 1000; i++) {
      s.patch(1);
    }

    assert.equal(s.delta(0,0), 0);
    assert.equal(s.delta(1,2), 1);
    assert.equal(s.delta(1,3), 2);
    assert.equal(s.delta(0,50), 50);
    assert.equal(s.delta(1,50), 50);
    assert.equal(s.delta(951, 1000), 50);
  });

  it('supports delta from a change number forward', function() {
    var a, b, c;
    s.patch(a = Math.random());
    s.patch(b = Math.random());
    s.patch(c = Math.random());
    assert.equal(c, s.delta(2));
    assert.equal(c, s.delta(2, 3));
  });

  it('has reasonable speed for 50,000 patches', function() {
    this.timeout(1000);
    for (var i=0; i < 50000; i++) {
      s.patch(1);
    }

    assert.equal(s.sum(49999), 49999);
  });

  it('works with non-trivial deltas', function() {
    var s = new Synopsis({
      start: {},
      patcher: function(doc, patch) {
        return jiff.patch(patch, doc);
      },
      differ: function(before, after) {
        return jiff.diff(before, after);
      }
    });

    s.patch([{op: 'add', path: '/a', value: 1}]);
    s.patch([{op: 'add', path: '/b', value: 2}]);
    s.patch([{op: 'add', path: '/c', value: 3}]);
    s.patch([{op: 'add', path: '/d', value: 4}]);

    assert.deepEqual(s.sum(), {a:1, b:2, c:3, d:4});
    assert.deepEqual(s.delta(1,3), [
      {op: 'add', path: '/c', value: 3},
      {op: 'add', path: '/b', value: 2}
    ]);
  });
});
