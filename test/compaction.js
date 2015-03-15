var assert = require('assert');
var async = require('neo-async');
var curry = require('curry');
var range = require('amp-range');
var aassert = require('./async-assert.js');

var Synopsis = require('../index.js');

describe('compaction', function () {
  var s;
  var memoryStore;

  var patchN1s = curry(function (n, cb) {
    async.eachSeries(range(n), function (n, cb) {
      s.patch(1, cb);
    }, cb);
  });

  beforeEach(function (done) {
    memoryStore = require('../stores/memory')();
    // Uber simple Synopsis example that just calculates the diff
    s = new Synopsis({
      start: 0,
      granularity: 2,
      store: memoryStore,
      patcher: function (prev, patch, cb) {
        if (patch === -2 /** Arbitrary to test failures */ ) {
          return cb(new Error('Invalid Patch'));
        }

        cb(null, prev + patch);
      },
      differ: function (before, after, cb) {
        cb(null, after - before);
      }
    });

    s.patch = curry(s.patch);
    s.delta = curry(s.delta);
    s.collectDeltas = curry(s.collectDeltas);
    s.compact = curry(s.compact);

    s.on('ready', function () {
      done();
    });
  });

  it('compacting an empty syn dones nothing', function (done) {
    s.compact(function (err) {
      assert(!err, 'no error expected');
      done();
    });
  });

  it('compacting a syn with one element does nothing', function (done) {
    s.patch(1, function () {
      s.compact(function (err) {
        s.delta(0, 1, function (err, delta) {
          assert.equal(delta, 1);
          done();
        });
      });
    });
  });

  it('compacting a syn updates individual patches', function (done) {
    patchN1s(2, function () {
      s.compact(function (err) {
        async.parallel([
          aassert.equal(s.delta(0, 1), 0),
          aassert.equal(s.delta(1, 2), 2),
        ], done);
      });
    });
  });

  it('compacting a single time works', function (done) {
    patchN1s(8, function () {
      s.compact(function (err) {
        async.parallel([
          aassert.equal(s.delta(0, 2), 2),
          aassert.equal(s.delta(1, 3), 3),
          aassert.equal(s.delta(0, 3), 3),
          aassert.equal(s.delta(0, 4), 4),
        ], done);
      });
    });
  });

  it('compacting granularity times works to update -1 intervals', function (done) {
    patchN1s(8, function () {
      repeat(s.compact(), 2, function (err) {
        async.parallel([
          aassert.equal(s.delta(0, 1), 0),
          aassert.equal(s.delta(1, 2), 0),
          aassert.equal(s.delta(2, 3), 3),
          aassert.equal(s.delta(3, 4), 1),
          aassert.equal(s.delta(4, 5), 1),
        ], done);
      });
    });
  });

  it('compacting granularity + 1 times works to update -1 intervals', function (done) {
    patchN1s(8, function () {
      repeat(s.compact(), 3, function (err) {
        async.parallel([
          aassert.equal(s.delta(0, 1), 0),
          aassert.equal(s.delta(1, 2), 0),
          aassert.equal(s.delta(2, 3), 0),
          aassert.equal(s.delta(3, 4), 4),
          aassert.equal(s.delta(4, 5), 1),
        ], done);
      });
    });
  });

  it('compacting right up to the end works as expected', function (done) {
    patchN1s(8, function () {
      var deltaRange = curry(function (start, end, size, cb) {
        async.reduce(range(start, end + 1 - size), [], function (deltas, index, cb) {
          s.delta(index, index + size, function (err, delta) {
            deltas.push(delta);
            cb(null, deltas);
          });
        }, cb);
      });

      async.series([
        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 2, 1, 1, 1, 1, 1, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [2, 3, 2, 2, 2, 2, 2]),
        aassert.deepEqual(deltaRange(0, 8, 3), [3, 4, 3, 3, 3, 3]),
        aassert.deepEqual(deltaRange(0, 8, 4), [4, 5, 4, 4, 4]),
        aassert.deepEqual(deltaRange(0, 8, 5), [5, 6, 5, 5]),
        aassert.deepEqual(deltaRange(0, 8, 6), [6, 7, 6]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),
        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 3, 1, 1, 1, 1, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 3, 4, 2, 2, 2, 2]),
        aassert.deepEqual(deltaRange(0, 8, 3), [3, 4, 5, 3, 3, 3]),
        aassert.deepEqual(deltaRange(0, 8, 4), [4, 5, 6, 4, 4]),
        aassert.deepEqual(deltaRange(0, 8, 5), [5, 6, 7, 5]),
        aassert.deepEqual(deltaRange(0, 8, 6), [6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),
        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 4, 1, 1, 1, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 4, 5, 2, 2, 2]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 4, 5, 6, 3, 3]),
        aassert.deepEqual(deltaRange(0, 8, 4), [4, 5, 6, 7, 4]),
        aassert.deepEqual(deltaRange(0, 8, 5), [5, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),
        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 0, 5, 1, 1, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 0, 5, 6, 2, 2]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 0, 5, 6, 7, 3]),
        aassert.deepEqual(deltaRange(0, 8, 4), [0, 5, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 5), [5, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),
        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 0, 0, 6, 1, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 0, 0, 6, 7, 2]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 0, 0, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 4), [0, 0, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 5), [0, 6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [6, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),

        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 0, 0, 0, 7, 1]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 0, 0, 0, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 0, 0, 0, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 4), [0, 0, 0, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 5), [0, 0, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [0, 7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [7, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),

        s.compact(),
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 4), [0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 5), [0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),

        s.compact(), // This should have no effect
        aassert.deepEqual(deltaRange(0, 8, 1), [0, 0, 0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 2), [0, 0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 3), [0, 0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 4), [0, 0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 5), [0, 0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 6), [0, 0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 7), [0, 8]),
        aassert.deepEqual(deltaRange(0, 8, 8), [8]),

      ], done);

    });
  });
});

function repeat(fn, count, cb) {
  async.whilst(function () {
    return count-- > 0;
  }, fn, cb);
}

function addPatches(s, patches, cb) {
  async.eachSeries(patches, s.patch, cb);
}
