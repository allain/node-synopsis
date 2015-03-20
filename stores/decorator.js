var async = require('neo-async');

/**
 * Patches the given stores so that they have all of the expected methods
 * by using the mandatory ones `set` and `get`
 */
module.exports = function(store) {
  if (!store.setAll) {
    store.setAll = function (map, cb) {
      async.eachSeries(Object.keys(map), function (key, cb) {
        store.set(key, map[key], cb);
      }, cb);
    };
  }

  if (!store.getAll) {
    store.getAll = function (keys, cb) {
      var result = {};
      async.eachSeries(keys, function (key, cb) {
        store.get(key,function(err, value) {
          if (err) return cb(err);
          result[key] = value;
          cb();
        });
      }, function (err) {
        if (err) return cb(err);

        cb(null, result);
      });
    };
  }

  //putAll treats values of undefined as deletes
  if (!store.putAll) {
    store.putAll = function (map, cb) {
      async.eachSeries(Object.keys(map), function (key, cb) {
        var val = map[key];
        if (val === undefined) {
          store.remove(key, cb);
        } else {
          store.set(key, val, cb);
        }
      }, cb);
    };
  }

  return store;
};
