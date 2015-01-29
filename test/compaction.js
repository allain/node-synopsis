var assert = require('assert');
var jiff = require('jiff');
var async = require('async');
var curry = require('curry');
var range = require('amp-range');
var Duplex = require('stream').Duplex;
var stdout = require('stdout');
var Synopsis = require('../index.js');
var Writable = require('stream').Writable;

function expectStream(expected, done) {
  assert(Array.isArray(expected));

  var stream = new Writable({
    objectMode: true
  });

  stream._write = function (chunk, encoding, cb) {
    if (expected.length === 0) {
      assert(false, 'write occurred after no writes expected');
    }

    var expectedChunk = expected.shift();
    assert.deepEqual(chunk, expectedChunk);

    if (expected.length === 0) {
      setTimeout(function () {
        done();
      }, 10);
    }

    cb();
  };

  return stream;
}

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

describe.skip('compaction', function () {
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
      granularity: 2,
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
  beforeEach(patchN1s(3));

  it('behaves sanely when no aggregate boundary passed', function (done) {
    s.compact(function (err) {
      async.parallel([
        asyncAssert.equal(s.delta(0, 1), 1),
        asyncAssert.equal(s.delta(0, 2), 2),
        asyncAssert.equal(s.delta(1, 2), 2)
      ], done);
    });
  });

  it('behaves sanely when aggregate boundary passed', function (done) {
    async.eachSeries([1, 2], function (c, cb) {
      s.compact(cb);
    }, function (err) {
      console.log('s.delta(0,2)');
      async.parallel([
        //asyncAssert.equal(s.delta(0,1), 0),
        //asyncAssert.equal(s.delta(0,2), 0),
        //asyncAssert.equal(s.delta(0,5), 0),
        //asyncAssert.equal(s.delta(5,6), 6),
        asyncAssert.equal(s.delta(0, 2), 2),
      ], done);
    });
  });
});

function addPatches(s, patches, cb) {
  async.eachSeries(patches, s.patch, cb);
}