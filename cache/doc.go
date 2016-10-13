// Package cache provide LRU cache for memcached protocol.
// Note: Doc based on github.com/memcached/memcached/doc/new_lru.txt
// * There are HOT, WARM, and COLD LRU's. New items enter the
// HOT LRU.
// * LRU updates only happen as items reach the bottom of an LRU. If active in
// HOT, stay in HOT, if active in WARM, stay in WARM. If active in COLD, move
// to WARM.
// * HOT/WARM each capped at 32% of memory available for that slab class. COLD
// is uncapped (by default, as of this writing).
// * Items flow from HOT/WARM into COLD.
//
// The primary goal is to better protect active items from "scanning". Items
// which are never hit again will flow from HOT, through COLD, and out the
// bottom. Items occasionally active (reaching COLD, but being hit before
// eviction), move to WARM. There they can stay relatively protected.
// A secondary goal is to improve latency. The LRU locks are no longer used on
// item reads, only during sets and deletes.
// TODO update doc after add LRU crawler.
package cache
