toy implementation of memcache. named after [3i/atlas](https://en.wikipedia.org/wiki/3I/ATLAS), the oldest comet discovered (yet).

it has a concurrent hashmap (implemented with lock striping) & a concurrent doubly linked list working together as an LRU cache.

it can distribute keys across a cluster of machines.
