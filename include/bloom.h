#pragma once
#include "common.h"

// ==========================================
// CLASS: BLOOM FILTER (Probabilistic Index)
// ==========================================
// Stored on Page 0 after the DbHeader.  Persisted automatically via page cache.
// On startup, rebuilt by scanning all leaf nodes (handles migration + stale bits
// from deletes).  3 hash functions → very low false-positive rate at ForgeDB scale.
//
// Layout of Page 0:
//   [DbHeader: 20 bytes][Bloom bit-array: 4076 bytes (32 608 bits)]

class BloomFilter {
    uint8_t* bits;

    uint32_t hash1(uint32_t k) const;
    uint32_t hash2(uint32_t k) const;
    uint32_t hash3(uint32_t k) const;

    void set_bit(uint32_t pos) { bits[pos / 8] |= (1 << (pos % 8)); }
    bool get_bit(uint32_t pos) const { return bits[pos / 8] & (1 << (pos % 8)); }

public:
    BloomFilter() : bits(nullptr) {}

    void attach(void* page0);
    void add(uint32_t key);

    // Returns TRUE  → "maybe present"  (must verify in B+Tree)
    // Returns FALSE → "definitely not present"  (skip B+Tree entirely)
    bool possibly_contains(uint32_t key) const;

    void clear();
    void print_stats() const;
};
