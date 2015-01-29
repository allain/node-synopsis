var assert = require('assert');
var curry = require('curry');

// Little utility that allows me to more easily test async functions
module.exports = {
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