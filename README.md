# memcached
Simple memcached server. Test task for working in VK.


## TODO
* test CLI helper functions
* RDB persistence
  * point-in-time snapshots of your dataset at specified intervals
  * current version already use snapshots to archive large AOF, so it is easy to implement such feature
* Profiling with pprof and optimisation
* Snapshot consistency check
  * Now check happen only in build with debug tag
* Optional
  * replace state snapshot usage as AOF rotation
