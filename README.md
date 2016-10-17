# memcached
Simple memcached server. 
[![Build Status](https://api.travis-ci.org/Skipor/memcached.svg)](https://travis-ci.org/Skipor/memcached)  [![GoDoc](https://godoc.org/github.com/Skipor/memcached?status.svg)](https://godoc.org/github.com/Skipor/memcached)  [![codecov](https://codecov.io/gh/Skipor/memcached/branch/master/graph/badge.svg)](https://codecov.io/gh/Skipor/memcached)

## Features

* No third party packages for app, server and cache logic.
  * That was tack constraint.
  * I have used only small github.com/facebookgo/stackerr package for errors with stack traces.
* Parallel reads, serialized writes.
* Items data sync.Pool recycle.
* Low allocation text protocol parse.
* AOF persistence with configurable sync options.
  * Every command can be synced, or sync can be done by ticker.
  * Low latency fast log rotation.
    * When log grows large, background goroutine takes fast snapshot under read lock.
    * Snapshot is writen without log lock.
* Many unit and integration tests.
    

## TODO
* LRU crawler
  * goroutine waking up on ticker; take RLock; start goroutine per queue; find expired; save them; take write lock and remove them.
* Refactor integration load tester. Do more load tests and comparation with original memcached.
* Test CLI helper functions
* RDB persistence
  * point-in-time snapshots of your dataset at specified intervals
  * current version already use snapshots to archive large AOF, so it is easy to implement such feature
* Profiling with pprof and optimisation
* Snapshot consistency check
  * Now check happen only in build with debug tag
* Optional
  * replace state snapshot usage as AOF rotation
