module.exports = function(options) {
	var cache = [];

	return {
		get: function(key, cb) {
			return cb(null, cache[key]);
		},
		set: function(key, value, cb) {
			cache[key] = value;
			cb();
		},
		setAll: function(map, cb) {
			var self = this;
      Object.keys(map).forEach(function(key) {
				cache[key] = map[key];		
			});
      cb();
		}
	};
};
