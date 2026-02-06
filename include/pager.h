#pragma once
#include "common.h"
#include <fstream>
#include <list>
#include <unordered_map>
#include <string>

// ==========================================
// CLASS: PAGER (Disk Manager + Buffer Pool)
// ==========================================
class Pager {
public:
    std::fstream file_stream;
    uint32_t file_length;
    DbHeader header;

    // === Buffer Pool (LRU Page Cache) ===
    // The on-disk file can grow without bound; only BUFFER_POOL_SIZE
    // frames are held in RAM.  When the pool is full, the Least Recently
    // Used page is evicted (flushed to disk if dirty) to make room.
    std::unordered_map<uint32_t, void*> pool;
    std::list<uint32_t> lru_order;   // front = MRU, back = LRU
    std::unordered_map<uint32_t, std::list<uint32_t>::iterator> lru_map;
    std::unordered_map<uint32_t, uint32_t> pin_counts;  // Pinned pages can't be evicted
    uint64_t stat_hits   = 0;
    uint64_t stat_misses = 0;
    uint64_t stat_evicts = 0;

    Pager(std::string filename);
    ~Pager();

    // --- Page Cache ---
    void* get_page(uint32_t page_num);
    void flush(uint32_t page_num);

    // --- LRU Eviction ---
    void evict_lru();

    // --- Page Pinning (prevents eviction) ---
    void pin_page(uint32_t page_num);
    void unpin_page(uint32_t page_num);
    bool is_pinned(uint32_t page_num) const;

    // --- Free List Management ---
    uint32_t get_unused_page_num();
    void free_page(uint32_t page_num);

    // --- Header Persistence ---
    void write_header();

    // --- Debug Helpers ---
    void print_stats();
    void print_free_list();
    void print_pool_stats();
};
