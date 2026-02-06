#include "bloom.h"
#include <iostream>
#include <cmath>
#include <cstdio>

// Three independent hash functions (multiplicative hashing with distinct primes)
uint32_t BloomFilter::hash1(uint32_t k) const { return (uint32_t)(((uint64_t)k * 2654435761ULL) % BLOOM_BITS); }
uint32_t BloomFilter::hash2(uint32_t k) const { return (uint32_t)(((uint64_t)k * 0x85ebca6bULL) % BLOOM_BITS); }
uint32_t BloomFilter::hash3(uint32_t k) const { return (uint32_t)(((uint64_t)(k ^ (k >> 16)) * 0xcc9e2d51ULL) % BLOOM_BITS); }

// Point the filter at the bloom area on page 0
void BloomFilter::attach(void* page0) {
    bits = (uint8_t*)page0 + BLOOM_OFFSET;
}

void BloomFilter::add(uint32_t key) {
    set_bit(hash1(key));
    set_bit(hash2(key));
    set_bit(hash3(key));
}

bool BloomFilter::possibly_contains(uint32_t key) const {
    return get_bit(hash1(key)) && get_bit(hash2(key)) && get_bit(hash3(key));
}

void BloomFilter::clear() {
    std::memset(bits, 0, BLOOM_SIZE);
}

void BloomFilter::print_stats() const {
    uint32_t set_count = 0;
    for (uint32_t i = 0; i < BLOOM_SIZE; i++) {
        uint8_t b = bits[i];
        while (b) { set_count += b & 1; b >>= 1; }
    }
    double fill = (double)set_count / BLOOM_BITS * 100.0;
    double fpr  = std::pow((double)set_count / BLOOM_BITS, 3) * 100.0;
    std::cout << "=== Bloom Filter ===\n";
    std::cout << "Size:     " << BLOOM_SIZE << " bytes (" << BLOOM_BITS << " bits)\n";
    std::cout << "Bits Set: " << set_count << " / " << BLOOM_BITS << "\n";
    std::printf("Fill:     %.1f%%\n", fill);
    std::printf("Est. FPR: ~%.4f%%\n", fpr);
}
