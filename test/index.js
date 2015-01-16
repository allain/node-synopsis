var assert = require('assert');
var jiff = require('jiff');
var async = require('async');
var _ = require('lodash');
var Duplex = require('stream').Duplex;

var Synopsis = require('../index.js');

// Little utility that allows me to more easily test async functions
var asyncAssert = {
  equal: _.curry(function(generator, expected, cb) {
    generator(function(err, actual) {
      try {
        assert.equal(actual, expected);
        cb();
      } catch(e) {
        cb(e);
      }
    });
  }),
  deepEqual: _.curry(function(generator, expected, cb) {
    generator(function(err, actual) {
      try {
        assert.equal(JSON.stringify(actual), JSON.stringify(expected));
        cb();
      } catch(e) {
        cb(e);
      }
    });
  })
};

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

    s.patch = _.curry(s.patch);
    s.delta = _.curry(s.delta);
    s.collectDeltas = _.curry(s.collectDeltas);
    s.size = _.curry(s.size);

    s.on('ready', function() {
      done();
    });
  });

  it('emits ready', function(done) {
    new Synopsis({
      start: 0,
      granularity: 5,
      patcher: function(prev, patch) {
        return prev + patch;
      },
      differ: function(before, after) {
        return after - before;
      }
    }).on('ready', function() {
      done();
    });
  });

  it('is sane when empty', function(done) {
    s.sum = _.curry(s.sum);

    async.parallel([
      asyncAssert.equal(s.sum(0), 0),
      asyncAssert.equal(s.size(), 0)
    ], done());
  });

  it('sum can accept no index', function(done) {
    s.sum(function(err, sum) {
      assert.equal(sum, 0);
      done();
    });
  });

  it('computes sum when patches are added', function(done) {
    async.series([
      s.patch(2),
      s.patch(3),
      asyncAssert.equal(s.size(), 2)
    ], function(err, cb) {
      assert(!err, err);

      s.sum(function(err, sum) {
        assert(!err, err);

        assert.equal(sum, 5);
        done();
      });
    });
  });


  it('can handle multiple simultaneous incoming patches', function(done) {
    s.sum = _.curry(s.sum);

    this.timeout(30000);

    var patchOps = [];
    async.parallel(_.range(0, 10000).map(function(n) {
      return function(cb) {
        setImmediate(function() {
          return s.patch((n % 2) * 2 - 1, cb);
        });
      };
    }), function(err) {
      if (err) done(err);

      async.parallel([
        asyncAssert.equal(s.sum(1000), 0),
        //asyncAssert.equal(s.size(), 1000),
      ], done);
    });
  });


  it('aggregates deltas', function(done) {
    var deltaCount = 5;
    async.whilst(function() {
      return deltaCount--;
    }, s.patch(1), function(err) {
      assert(!err, err);
      s.delta(0, 5, function(err, delta) {
        assert(!err, err);
        assert.equal(delta, 5);
        done();
      });
    });
  });



  it('sum behaves as expected', function(done) {
    s.sum = _.curry(s.sum);

    async.eachSeries(_.range(1,5),
      s.patch,
      function(err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.equal(s.sum(1), 1),
        asyncAssert.equal(s.sum(2), 3),
        asyncAssert.equal(s.sum(3), 6),
        asyncAssert.equal(s.sum(4), 10),
        asyncAssert.equal(s.delta(1, 2), 2),
        asyncAssert.equal(s.delta(1, 3), 5),
        asyncAssert.equal(s.delta(1, 4), 9),
        asyncAssert.equal(s.delta(2, 4), 7)
      ], done);
    });
  });

  it('uses delta merging', function(done) {
    s.sum = _.curry(s.sum);

    patchN1s(125, function(err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.equal(s.sum(5), 5),
        asyncAssert.deepEqual(s.collectDeltas(0,4), [1,1,1,1]),
        asyncAssert.deepEqual(s.collectDeltas(0,5), [5]),
        asyncAssert.deepEqual(s.collectDeltas(0,6), [5,1]),
        asyncAssert.deepEqual(s.collectDeltas(0,10), [5,5]),
        asyncAssert.deepEqual(s.collectDeltas(0,124), [25,25,25,25,5,5,5,5,1,1,1,1]),
        asyncAssert.deepEqual(s.collectDeltas(0,125), [125]),
      ], done);
    });
  });

  it('supports delta from any two points in time', function(done) {
    patchN1s(1000, function(err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.equal(s.delta(0,0), 0),
        asyncAssert.equal(s.delta(0,1), 1),
        asyncAssert.equal(s.delta(1,2), 1),
        asyncAssert.equal(s.delta(0,50), 50),
        asyncAssert.equal(s.delta(1,50), 49),
        asyncAssert.equal(s.delta(951,1000), 49)
      ], done);
    });
  });

  var hardCount = 1000;
  it('has reasonable speed for ' + hardCount + ' patches', function(done) {
    this.timeout(10000);

    var start = Date.now();
    patchN1s(hardCount, function(err) {
      assert(!err, err);
      s.sum(hardCount - 1, function(err, sum) {
        assert.equal(sum, hardCount-1);

        assert(Date.now() - start < 1000);
        done();
      });
    });
  });

  it('emits patched events', function(done) {
    s.on('patched', function(p) {
      assert.equal(p, 1);
      done();
    });

    s.patch(1, function(err) {
      assert(!err, err);
    });
  });

  it('size works', function(done) {
    async.series([
      asyncAssert.equal(s.size(), 0),
      s.patch(1),
      //asyncAssert.equal(s.size(), 1),
    /*  asyncAssert.equal(s.size(), 1),
      s.patch(2),
      asyncAssert.equal(s.size(), 2)*/
    ], done);
  });

  it('works with non-trivial deltas', function(done) {
    this.timeout(3000000);
    var s = new Synopsis({
      start: {},
      granularity: 2,
      patcher: function(doc, patch) {
        return jiff.patch(patch, doc);
      },
      differ: function(before, after) {
        return jiff.diff(before, after);
      }
    });

    s.sum = _.curry(s.sum);
    s.delta = _.curry(s.delta);
    s.collectDeltas = _.curry(s.collectDeltas);

    addPatches(s, [
      [{op: 'add', path: '/a', value: [1]}],
      [{op: 'add', path: '/a/1', value: 2}],
      [{op: 'add', path: '/a/2', value: 3}],
      [{op: 'add', path: '/a/3', value: 4}],
      [{op: 'add', path: '/a/4', value: 5}],
      [{op: 'add', path: '/a/5', value: 6}],
      [{op: 'add', path: '/a/6', value: 7}],
      [{op: 'add', path: '/a/7', value: 8}]
    ], function(err) {
      assert(!err, err);

      async.parallel([
        asyncAssert.deepEqual(s.sum(0), {}),
        asyncAssert.deepEqual(s.sum(1), {a: [1]}),
        asyncAssert.deepEqual(s.delta(0, 1), [
          {op: 'add', path: '/a', value: [1]}
        ]),
        asyncAssert.deepEqual(s.collectDeltas(0, 1), [
          [{op: 'add', path: '/a', value: [1]}]
        ]),
        asyncAssert.deepEqual(s.delta(1, 3), [
          {op: 'add', path: '/a/1', value: 2, context: undefined},
          {op: 'add', path: '/a/2', value: 3, context: undefined}
        ]),
        asyncAssert.deepEqual(s.sum(5), {a: [1,2,3,4,5]}),
        asyncAssert.deepEqual(s.sum(8), {a: [1,2,3,4,5,6,7,8]}),
        asyncAssert.deepEqual(s.collectDeltas(2, 3), [[
          {op: 'add', path: '/a/2', value: 3, context: undefined}
        ]]),
        asyncAssert.deepEqual(s.collectDeltas(2, 4), [[
          {op: 'add', path: '/a/2', value: 3, context: undefined},
          {op: 'add', path: '/a/3', value: 4, context: undefined}
        ]]),
        asyncAssert.deepEqual(s.collectDeltas(3, 4), [[
          {op: 'add', path: '/a/3', value: 4, context: undefined}
        ]]),
        asyncAssert.deepEqual(s.collectDeltas(3, 5), [
          [{op: 'add', path: '/a/3', value: 4, context: undefined}],
          [{op: 'add', path: '/a/4', value: 5, context: undefined}]
        ]),
        asyncAssert.deepEqual(s.collectDeltas(5,8), [
          [
            {op: 'add', path: '/a/5', value: 6, context: undefined}
          ],
          [
            {op: 'add', path: '/a/6', value: 7, context: undefined},
            {op: 'add', path: '/a/7', value: 8, context: undefined}
          ]
        ]),
        asyncAssert.deepEqual(s.delta(5,8), [
          {op: 'add', path: '/a/5', value: 6, context: undefined},
          {op: 'add', path: '/a/6', value: 7, context: undefined},
          {op: 'add', path: '/a/7', value: 8, context: undefined}
        ]),
      ], done);
    });
  });

  describe('streaming', function() {
    beforeEach(function(done) {
      patchN1s(100, done);
    });

    it('sends first delta without having to write to it first', function(done) {
      s.createStream(function(err, stream) {
        stream.on('data', function(update) {
          assert.deepEqual(update, [100, 100]);
          done();
        });
      });
    });

    it('sends no updates unless changes occur', function(done) {
      s.createStream(function(err, stream) {
        var expectedData = [[100, 100]];
        stream.on('data', function(update) {
          assert.deepEqual(update, expectedData.shift());
          setTimeout(function() {
            done();
          }, 25);
        });
      });
    });

    it('supports creation of duplex stream', function(done) {
      s.createStream(function(err, stream) {
        assert(!err);
        assert(stream instanceof Duplex);

        var expectedData = [
        [100, 100], // includes everything from the start (0)
        [2, 101] // only includes the 2 patch from below
        ];

        stream.on('data', function(update) {
          var expected = expectedData.shift();

          assert.deepEqual(update, expected);
          if (expectedData.length === 0) {
            done();
          }
        });


        s.patch(2, function(err) {
          assert(!err, err);
        });
      });
    });

    it('supports specifying a start index', function(done) {
      s.createStream(50, function(err, stream) {
        assert(!err);
        assert(stream instanceof Duplex);

        var expectedData = [
        [50, 100], // includes everything from the start (0)
        [2, 101] // only includes the 2 patch from below
        ];

        stream.on('data', function(update) {
          var expected = expectedData.shift();

          assert.deepEqual(update, expected);
          if (expectedData.length === 0) {
            done();
          }
        });


        s.patch(2, function(err) {
          assert(!err, err);
        });
      });
    });

    it.skip('releases resources when stream closes', function(done) {
      // TODO
    });
  });

  function patchN1s(n, cb) {
    async.eachSeries(_.range(0, n), function(n, cb) { s.patch(1, cb); }, cb);
  }

});

function addPatches(s, patches, cb) {
  async.eachSeries(patches, s.patch, cb);
}
