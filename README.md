# memcached
[![Build Status](https://api.travis-ci.org/Skipor/memcached.svg)](https://travis-ci.org/Skipor/memcached)
Simple memcached server. Test task for VK job.



## TODO
* LRU crawler
  * goroutine waking up on ticker; take RLock; start goroutine per queue; find expired; save them; take write lock and remove them.
* Refactor integration load tester. Do more load tests and comparation with original memcached.
* test CLI helper functions
* RDB persistence
  * point-in-time snapshots of your dataset at specified intervals
  * current version already use snapshots to archive large AOF, so it is easy to implement such feature
* Profiling with pprof and optimisation
* Snapshot consistency check
  * Now check happen only in build with debug tag
* Optional
  * replace state snapshot usage as AOF rotation
