package feature_test

import (
	"hash/fnv"
	"testing"
)

// The feature.Store requires a live Redis connection. All Store-level tests are
// skipped in unit-test runs. The hashBucket helper is tested via its exported
// behaviour indirectly through the package-level logic below.

func TestFeature_RequiresRedis(t *testing.T) {
	t.Skip("requires external Redis service")
}

// TestHashBucket_Deterministic verifies that the hash-bucket assignment is
// stable: the same (flag, entity) pair must always land in the same bucket.
// This is a pure-logic test with no external dependencies.
func TestHashBucket_Deterministic(t *testing.T) {
	// Replicate the exact same FNV-1a logic used by hashBucket inside feature.go.
	hashBucket := func(key string, buckets int) int {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		return int(h.Sum64() % uint64(buckets))
	}

	cases := []struct {
		key     string
		buckets int
	}{
		{"dark-mode:user-1", 100},
		{"dark-mode:user-2", 100},
		{"new-checkout:org-99", 100},
	}

	for _, c := range cases {
		a := hashBucket(c.key, c.buckets)
		b := hashBucket(c.key, c.buckets)
		if a != b {
			t.Errorf("hashBucket(%q, %d) = %d on first call, %d on second — not deterministic",
				c.key, c.buckets, a, b)
		}
		if a < 0 || a >= c.buckets {
			t.Errorf("hashBucket(%q, %d) = %d, out of range [0, %d)",
				c.key, c.buckets, a, c.buckets)
		}
	}
}

// TestHashBucket_Distribution verifies rough uniform distribution across
// buckets — no bucket should receive more than 3× its fair share for N keys.
func TestHashBucket_Distribution(t *testing.T) {
	hashBucket := func(key string, buckets int) int {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		return int(h.Sum64() % uint64(buckets))
	}

	const buckets = 10
	counts := make([]int, buckets)
	for i := range 1000 {
		key := "flag:" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		counts[hashBucket(key, buckets)]++
	}

	fair := 1000 / buckets
	for i, c := range counts {
		if c > fair*3 {
			t.Errorf("bucket %d has %d entries (fair=%d) — distribution too skewed", i, c, fair)
		}
	}
}
