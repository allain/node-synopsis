var assert = require('assert');
var jiff = require('jiff');
var async = require('async');
var curry = require('curry');
var range = require('amp-range');
var Duplex = require('stream').Duplex;
var stdout = require('stdout');
var Synopsis = require('../index.js');
var Writable = require('stream').Writable;

// Little utility that allows me to more easily test async functions
var asyncAssert = {
  equal: curry(function (generator, expected, cb) {
    generator(function (err, actual) {
      try {
        assert.equal(actual, expected);
        cb();
      } catch (e) {
        cb(e);
      }
    });
  }),
  deepEqual: curry(function (generator, expected, cb) {
    generator(function (err, actual) {
      try {
        assert.equal(JSON.stringify(actual), JSON.stringify(expected));
        cb();
      } catch (e) {
        cb(e);
      }
    });
  })
};

describe('basic usage', function () {
  var s;

  var patchN1s = curry(function (n, cb) {
    async.eachSeries(range(n), function (n, cb) {
      s.patch(1, cb);
    }, cb);
  });

  beforeEach(function (done) {
    // Uber simple Synopsis example that just calculates the diff
    s = new Synopsis({
      start: 0,
      granularity: 5,
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
    s.size = curry(s.size);

    s.on('ready', function () {
      done();
    });
  });

  it('emits ready', function (done) {
    new Synopsis({
      start: 0,
      granularity: 5,
      patcher: function (prev, patch, cb) {
        cb(null, prev + patch);
      },
      differ: function (before, after, cb) {
        cb(null, after - before);
      }
    }).on('ready', function () {
      done();
    });
  });

  it('is sane when empty', function (done) {
    s.snapshot = curry(s.snapshot);

    async.parallel([
      asyncAssert.equal(s.snapshot(0), 0),
      asyncAssert.equal(s.size(), 0)
    ], done());
  });

  it('snapshot can accept no index', function (done) {
    s.snapshot(function (err, snapshot) {
      assert.equal(snapshot, 0);
      done();
    });
  });

  it('computes snapshot when patches are added', function (done) {
    async.series([
      s.patch(2),
      s.patch(3),
      asyncAssert.equal(s.size(), 2)
    ], function (err, cb) {
      assert(!err, err);

      s.snapshot(function (err, snapshot) {
        assert(!err, err);
        assert.equal(snapshot, 5);
        done();
      });
    });
  });

  it('can handle multiple simultaneous incoming patches', function (done) {
    s.snapshot = curry(s.snapshot);

    this.timeout(30000);

    var patchOps = [];
    async.parallel(range(1000).map(function (n) {
      return function (cb) {
        setImmediate(function () {
          var val = (n % 2) * 2 - 1;
          assert(val === -1 || val === 1);
          return s.patch(val, cb);
        });
      };
    }), function (err) {
      if (err) done(err);

      async.parallel([
        asyncAssert.equal(s.snapshot(1000), 0),
        asyncAssert.equal(s.size(), 1000),
      ], done);
    });
  });

  it('aggregates deltas', function (done) {
    var deltaCount = 5;
    async.whilst(function () {
      return deltaCount--;
    }, s.patch(1), function (err) {
      assert(!err, err);
      s.delta(0, 5, function (err, delta) {
        assert(!err, err);
        assert.equal(delta, 5);
        done();
      });
    });
  });

  it('snapshot behaves as expected', function (done) {
    s.snapshot = curry(s.snapshot);

    async.eachSeries(range(1, 5),
      s.patch,
      function (err) {
        assert(!err, err);

        async.parallel([
          asyncAssert.equal(s.snapshot(1), 1),
          asyncAssert.equal(s.snapshot(2), 3),
          asyncAssert.equal(s.snapshot(3), 6),
          asyncAssert.equal(s.snapshot(4), 10),
          asyncAssert.equal(s.delta(1, 2), 2),
          asyncAssert.equal(s.delta(1, 3), 5),
          asyncAssert.equal(s.delta(1, 4), 9),
          asyncAssert.equal(s.delta(2, 4), 7)
        ], done);
      });
  });

  it('uses delta merging', function (done) {
    s.snapshot = curry(s.snapshot);

    patchN1s(125, function (err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.equal(s.snapshot(5), 5),
        asyncAssert.deepEqual(s.collectDeltas(0, 4), [1, 1, 1, 1]),
        asyncAssert.deepEqual(s.collectDeltas(0, 5), [5]),
        asyncAssert.deepEqual(s.collectDeltas(0, 6), [5, 1]),
        asyncAssert.deepEqual(s.collectDeltas(0, 10), [5, 5]),
        asyncAssert.deepEqual(s.collectDeltas(0, 124), [25, 25, 25, 25, 5, 5, 5, 5, 1, 1, 1, 1]),
        asyncAssert.deepEqual(s.collectDeltas(0, 125), [125]),
      ], done);
    });
  });

  it('supports delta from any two points in time', function (done) {
    patchN1s(1000, function (err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.equal(s.delta(0, 0), 0),
        asyncAssert.equal(s.delta(0, 1), 1),
        asyncAssert.equal(s.delta(1, 2), 1),
        asyncAssert.equal(s.delta(0, 50), 50),
        asyncAssert.equal(s.delta(1, 50), 49),
        asyncAssert.equal(s.delta(951, 1000), 49)
      ], done);
    });
  });

  var hardCount = 10000;
  it('has reasonable speed for ' + hardCount + ' patches', function (done) {
    this.timeout(10000);

    var start = Date.now();
    patchN1s(hardCount, function (err) {
      assert(!err, err);
      s.snapshot(hardCount - 1, function (err, snapshot) {
        assert.equal(snapshot, hardCount - 1);

        assert(Date.now() - start < 10000);
        done();
      });
    });
  });

  it('emits patched events', function (done) {
    s.on('patched', function (p) {
      assert.equal(p, 1);
      done();
    });

    s.patch(1, function (err) {
      assert(!err, err);
    });
  });

  it('size works', function (done) {
    async.series([
      asyncAssert.equal(s.size(), 0),
      s.patch(1),
      asyncAssert.equal(s.size(), 1),
      s.patch(2),
      asyncAssert.equal(s.size(), 2)
    ], done);
  });
});

function addPatches(s, patches, cb) {
  async.eachSeries(patches, s.patch, cb);
}