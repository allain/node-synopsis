#synposis

Synopsis is a tool computing the effective deltas two points in time when all you are given is incremental updates.

The approach I'm taking is to take a granularity N, and then merge N consecutive deltas together into deltaN.

Then I do the same for NxN, NxNxN, etc. until the granularity is greater than the number of deltas.

Simple Counting Example:

``` js
// Uber simple Synopsis example that just calculates a running
// total of a sum of integers with granularity of 5
var s = new Synopsis({
  start: 0,
  granularity: 5,
  patcher: function(prev, patch) {
    return prev + patch;
  },
  differ: function(before, after) {
    return after - before;
  }
});

for (var i=0; i<1000; i++) {
  s.patch(i);
}

console.log(s.sum()); // Yields 1000
console.log(s.sum(500)); // Yields 500
console.log(s.delta(0,10)); // Yields 10
console.log(s.delta(5, 7)); // Yilds 2
```
