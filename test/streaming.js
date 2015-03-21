var assert = require('assert');
var async = require('neo-async');
var curry = require('curry');
var range = require('amp-range');
var Duplex = require('stream').Duplex;
var Synopsis = require('../index.js');
var Writable = require('stream').Writable;

var aassert = require('./async-assert');

function expectStream(expected, done) {
  assert(Array.isArray(expected));

  var stream = new Writable({
    objectMode: true
  });

  stream._write = function(chunk, encoding, cb) {
    if (expected.length === 0) {
      assert(false, 'write occurred after no writes expected');
    }

    var expectedChunk = expected.shift();
    assert.deepEqual(chunk, expectedChunk);

    if (expected.length === 0) {
      setTimeout(function() {
        done();
      }, 10);
    }

    cb();
  };

  return stream;
}

describe('streaming', function() {
  var s;

  var patchN1s = curry(function(n, cb) {
    async.eachSeries(range(n), function(n, cb) {
      s.patch(1, cb);
    }, cb);
  });

  beforeEach(function(done) {
    // Uber simple Synopsis example that just calculates the diff
    s = new Synopsis({
      start: 0,
      granularity: 5,
      patcher: function(prev, patch, cb) {
        if (patch === -2 /** Arbitrary to test failures */) {
          return cb(new Error('Invalid Patch'));
        }

        cb(null, prev + patch);
      },
      differ: function(before, after, cb) {
        cb(null, after - before);
      }
    });

    s.patch = curry(s.patch);
    s.delta = curry(s.delta);
    s.collectDeltas = curry(s.collectDeltas);
    s.compact = curry(s.compact);

    s.on('ready', function() {
      done();
    });
  });

  beforeEach(patchN1s(100));

  it('sends first delta without having to write to it first', function(done) {
    s.createStream(function(err, stream) {
      stream.on('data', function(update) {
        assert.deepEqual(update, [100, 100]);
        done();
      });
    });
  });

  it('sends full patch if handshake is older than tail', function(done) {
    s = new Synopsis({
      start: 0,
      granularity: 5,
      patcher: function(prev, patch, cb) {
        if (patch === -2 /** Arbitrary to test failures */) {
          return cb(new Error('Invalid Patch'));
        }

        cb(null, prev + patch);
      },
      differ: function(before, after, cb) {
        cb(null, after - before);
      }
    });

    s.patch = curry(s.patch);
    s.delta = curry(s.delta);
    s.collectDeltas = curry(s.collectDeltas);
    s.compact = curry(s.compact);
    s.stats = curry(s.stats);

    s.on('ready', function() {
      async.series([
        s.patch(1),
        s.patch(1),
        s.patch(1),
        s.patch(1),
        s.patch(1),
        s.compact(),
        s.compact(),
        s.compact(),
        s.compact(),
        s.patch(1),
        aassert.deepEqual(s.stats(), {
          head: 6,
          tail: 4
        })
      ], function(err) {
        assert(!err, err);
        s.createStream(2, function(err, stream) {
          stream.pipe(expectStream([
            [6, 6, 'reset']
          ], done));
        });
      });
    });

  });

  it('sends no updates unless changes occur', function(done) {
    s.createStream(function(err, stream) {
      stream.pipe(expectStream([
        [100, 100]
      ], done));
    });
  });

  it('supports creation of duplex stream', function(done) {
    s.createStream(function(err, stream) {
      assert(!err);
      assert(stream instanceof Duplex);

      stream.pipe(expectStream([
        [100, 100], // includes everything from the start (0)
        [2, 101] // only includes the 2 patch from below
      ], done));

      s.patch(2, function(err) {
        assert(!err, err);
      });
    });
  });

  it('supports specifying a start index', function(done) {
    s.createStream(50, function(err, stream) {
      stream.pipe(expectStream([
        [50, 100], // includes everything from the start (0)
        [2, 101] // only includes the 2 patch from below
      ], done));

      s.patch(2, function(err) {
        assert(!err, err);
      });
    });
  });

  it('emits nothing on startup if index is already at end', function(done) {
    s.createStream(100, function(err, stream) {
      var result = stream.read();
      assert.equal(result, null);
      done();
    });
  });

  it('a failing patch causes a notification to be emitted', function(done) {
    s.createStream(100, function(err, stream) {
      if (err) {
        assert.fail('no exception expected here');
      }
      stream.pipe(expectStream([{
        error: 'patch failed',
        patch: -2,
        cause: 'Invalid Patch'
      }], done));

      stream.write(-2);
    });
  });

  // Disabling until I figure out how to properly test backpressure
  it.skip('pausing stream causes effective delta to be sent when resumed', function(done) {
    s.createStream(function(err, stream) {
      assert(!err);
      assert(stream instanceof Duplex);

      stream.once('data', function(update) {
        assert.deepEqual(update, [100, 100]);
        stream.pause();
        stream.once('data', function(update) {
          assert.deepEqual(update, [2, 102]);
          done();
        });

        s.patch(1, function(err) {
          s.patch(1, function(err) {
            stream.resume();
          });
        });
      });
    });
  });
});

function addPatches(s, patches, cb) {
  async.eachSeries(patches, s.patch, cb);
}
