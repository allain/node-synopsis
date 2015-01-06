var assert = require('assert');
var jiff = require('jiff');
var async = require('async');
var _ = require('lodash');

var Synopsis = require('../index.js');

describe('Synopsis', function() {
  var s;
  beforeEach(function(done) {
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

    done();
  });

  it('is sane when empty', function(done) {
    s.sum(0, function(err, sum) {
      assert.equal(sum, 0);
      done();
    });
  });

  it('sum can accept no index', function(done) {
    s.sum(function(err, sum) {
      assert.equal(sum, 0);
      done();
    });
  });

  it('computes sum when patches are added', function(done) {
    async.series([
      function(cb) { s.patch(2, cb); },
      function(cb) { s.patch(3, cb); },
      function(cb) {
        s.sum(function(err, sum) {
          assert(!err, err);

          assert.equal(sum, 5);
          done();
          cb();
        });
      }
    ]);
  });


  it('aggregates deltas', function(done) {
    var deltaCount = 5;
    async.whilst(function() {
      return deltaCount--;
    }, function(cb) {
      s.patch(1, cb);
    }, function(err) {
      assert(!err, err);
      s.delta(0, 5, function(err, delta) {
        assert(!err, err);
        assert.equal(delta, 5);
        done();
      });
    });
  });



  it('sum behaves as expected', function(done) {
    async.eachSeries(_.range(1,5), function(n, cb) {
      s.patch(n, cb);
    }, function(err) {
      assert(!err, err);

      async.parallel({
        s1: function(cb) { s.sum(1, cb); },
        s2: function(cb) { s.sum(2, cb); },
        s3: function(cb) { s.sum(3, cb); },
        s4: function(cb) { s.sum(4, cb); },
        s: function(cb) { s.sum(cb); },
        d12: function(cb) { s.delta(1, 2, cb); },
        d13: function(cb) { s.delta(1, 3, cb); },
        d14: function(cb) { s.delta(1, 4, cb); },
        d24: function(cb) { s.delta(2, 4, cb); }
      }, function(err, r) {
        assert(!err, err);

        assert.equal(1, r.s1);
        assert.equal(3, r.s2);
        assert.equal(6, r.s3);
        assert.equal(10, r.s4);
        assert.equal(10, r.s);
        assert.equal(r.s2 - r.s1, r.d12);
        assert.equal(r.s3 - r.s1, r.d13);
        assert.equal(r.s4 - r.s1, r.d14);
        assert.equal(r.s4 - r.s2, r.d24);

        done();
      });
    });
  });

  it('uses delta merging', function(done) {
    patchN1s(125, function(err) {
      assert(!err, err);

      async.parallel({
        s5: function(cb) { s.sum(5, cb); },
        ds4: function(cb) { s.collectDeltas(0, 4, cb); },
        ds5: function(cb) { s.collectDeltas(0, 5, cb); },
        ds6: function(cb) { s.collectDeltas(0, 6, cb); },
        ds10: function(cb) { s.collectDeltas(0, 10, cb); },
        ds125: function(cb) { s.collectDeltas(0, 125, cb); },
        ds124: function(cb) { s.collectDeltas(0, 124, cb); }
      }, function(err, r) {
        assert.equal(r.s5, 5);
        assert.deepEqual(r.ds4, [1,1,1,1]);
        assert.deepEqual(r.ds5, [5]);
        assert.deepEqual(r.ds6, [5,1]);
        assert.deepEqual(r.ds10, [5,5]);
        assert.deepEqual(r.ds125, [125]);
        assert.deepEqual(r.ds124, [25,25,25,25,5,5,5,5,1,1,1,1]);
        done();
      });
    });
  });

  it('supports delta from any two points in time', function(done) {
    patchN1s(1000, function(err) {
      assert(!err, err);

      async.parallel({
        d0t0: function(cb) { s.delta(0, 0, cb); },
        d1t2: function(cb) { s.delta(1, 2, cb); },
        d1t3: function(cb) { s.delta(1, 3, cb); },
        d0t50: function(cb) { s.delta(0, 50, cb); },
        d1t50: function(cb) { s.delta(1, 50, cb); },
        d951t1000: function(cb) { s.delta(951, 1000, cb); },
      }, function(err, r) {
        assert(!err, err);

        assert.equal(r.d0t0, 0);
        assert.equal(r.d1t2, 1);
        assert.equal(r.d1t3, 2);
        assert.equal(r.d0t50, 50);
        assert.equal(r.d1t50, 50);
        assert.equal(r.d951t1000, 50);
        done();
      });
    });
  });

  var hardCount = 20000;
  it('has reasonable speed for ' + hardCount + ' patches', function(done) {
    this.timeout(1000);

    patchN1s(hardCount, function(err) {
      assert(!err, err);
      s.sum(hardCount-1, function(err, sum) {
        assert.equal(sum, hardCount-1);
        done();
      });
    });
  });

  it('rollup works', function(done) {
    async.series([
      function(cb) {
        patchN1s(10, cb);
      },
      function(cb) {
        s.rollup(5, function(err, result) {
          assert.deepEqual(result, {
            patch: 5,
            end: 10
          });
          cb();
        });
      },
      function(cb) {
        s.rollup(10, function(err, result) {
          assert.deepEqual(result, {
            patch: 0,
            end: 10
          });
          cb();
        });
      }
    ], done);
  });

  it('works with non-trivial deltas', function(done) {
    var s = new Synopsis({
      start: {},
      patcher: function(doc, patch) {
        return jiff.patch(patch, doc);
      },
      differ: function(before, after) {
        return jiff.diff(before, after);
      }
    });

    async.eachSeries([
      [{op: 'add', path: '/a', value: 1}],
      [{op: 'add', path: '/b', value: 2}],
      [{op: 'add', path: '/c', value: 3}],
      [{op: 'add', path: '/d', value: 4}]
    ], s.patch, function(err) {
      assert(!err, err);

      async.parallel({
        s0: function(cb) { s.sum(0, cb); },
        s: function(cb) { s.sum(cb); },
        d1: function(cb) {
          assert(typeof cb === 'function', 'd1 cb not a function');
          s.collectDeltas(0, 1, cb);
        },
        s1: function(cb) { s.sum(1, cb); },
        d1t3: function(cb) { s.delta(1, 3, cb); }
      }, function(err, r) {
        assert.deepEqual({}, r.s0);
        assert.deepEqual([[{op: 'add', path: '/a', value: 1}]], r.d1);
        assert.deepEqual({a: 1}, r.s1);
        assert.deepEqual(r.s, {a:1, b:2, c:3, d:4});
        assert.deepEqual(r.d1t3, [
          {op: 'add', path: '/c', value: 3},
          {op: 'add', path: '/b', value: 2}
        ]);

        done();
      });
    });
  });

  function patchN1s(n, cb) {
    async.map(_.range(1, n+1), function(n, cb) { s.patch(1, cb); }, cb);
  }
});
