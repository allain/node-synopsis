module.exports = function (options) {
  var cache = [];

  return {
    get: function (key, cb) {
      return cb(null, cache[key]);
    },
    set: function (key, value, cb) {
      cache[key] = value;
      cb();
    },
    remove: function (key, cb) {
      delete cache[key];
      cb();
    }
  };
};