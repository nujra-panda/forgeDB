#include "pager.h"
#include "utils.h"
#include <iostream>
#include <cstdlib>
#include <cstdio>

// ==========================================
// PAGER IMPLEMENTATION
// ==========================================

Pager::Pager(std::string filename) {
    // Open / Create file
    std::ifstream check(filename);
    if (!check.good()) {
        std::ofstream create(filename);
        create.close();
    }
    check.close();

    file_stream.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    file_stream.seekp(0, std::ios::end);
    file_length = file_stream.tellp();

    if (file_length == 0) {
        // --- New database: initialize header at page 0 ---
        header.magic = DB_MAGIC;
        header.page_size = PAGE_SIZE;
        header.total_pages = 1;  // Only the header page itself
        header.free_pages = 0;
        header.first_free_page = 0;
        write_header();
    } else {
        // --- Existing database: read & validate header ---
        void* page0 = get_page(HEADER_PAGE);
        std::memcpy(&header, page0, sizeof(DbHeader));

        if (header.magic != DB_MAGIC) {
            std::cerr << "ERROR: Invalid database file (bad magic 0x"
                      << std::hex << header.magic << std::dec << ").\n"
                      << "Delete the old .db file and restart.\n";
            std::exit(1);
        }
    }
    // Pin page 0 permanently — header + bloom filter always in RAM
    pin_page(HEADER_PAGE);
}

Pager::~Pager() {
    write_header();  // Persist latest header state
    // Flush every cached page and release memory
    for (auto& [pg, data] : pool) {
        flush(pg);
        std::free(data);
    }
    pool.clear();
    lru_order.clear();
    lru_map.clear();
    file_stream.close();
}

// --- Page Cache ---

void* Pager::get_page(uint32_t page_num) {
    // --- Cache HIT: page already in buffer pool ---
    auto it = pool.find(page_num);
    if (it != pool.end()) {
        stat_hits++;
        // Promote to MRU position
        lru_order.erase(lru_map[page_num]);
        lru_order.push_front(page_num);
        lru_map[page_num] = lru_order.begin();
        return it->second;
    }

    // --- Cache MISS ---
    stat_misses++;

    // Evict LRU page(s) if pool is full
    while (pool.size() >= BUFFER_POOL_SIZE) {
        evict_lru();
    }

    // Allocate frame and read from disk if page exists on file
    void* page = std::calloc(1, PAGE_SIZE);
    uint32_t file_pages = file_length / PAGE_SIZE;
    if (file_length % PAGE_SIZE) file_pages++;

    if (page_num < file_pages) {
        file_stream.seekg(page_num * PAGE_SIZE);
        file_stream.read((char*)page, PAGE_SIZE);
        file_stream.clear();  // Clear EOF flags

        // Verify CRC32 for tree pages (skip header page 0 and freed pages)
        if (page_num > HEADER_PAGE) {
            uint8_t page_type = *((uint8_t*)page);
            if (page_type == NODE_LEAF || page_type == NODE_INTERNAL) {
                uint32_t stored;
                std::memcpy(&stored, (char*)page + OFFSET_CHECKSUM, 4);
                if (stored != 0) {
                    uint32_t* crc_field = (uint32_t*)((char*)page + OFFSET_CHECKSUM);
                    *crc_field = 0;
                    uint32_t computed = crc32_compute((uint8_t*)page, PAGE_SIZE);
                    *crc_field = stored;
                    if (stored != computed) {
                        std::cerr << "WARNING: CRC32 mismatch on Page " << page_num
                                  << " (stored=0x" << std::hex << stored
                                  << " computed=0x" << computed << std::dec << ")\n";
                    }
                }
            }
        }
    }

    // Insert into pool + LRU list
    pool[page_num] = page;
    lru_order.push_front(page_num);
    lru_map[page_num] = lru_order.begin();
    return page;
}

void Pager::flush(uint32_t page_num) {
    auto it = pool.find(page_num);
    if (it == pool.end()) return;
    void* data = it->second;
    // Stamp CRC32 into tree pages before writing (skip header and free pages)
    if (page_num > HEADER_PAGE) {
        uint8_t page_type = *((uint8_t*)data);
        if (page_type == NODE_LEAF || page_type == NODE_INTERNAL) {
            uint32_t* crc_field = (uint32_t*)((char*)data + OFFSET_CHECKSUM);
            *crc_field = 0;
            *crc_field = crc32_compute((uint8_t*)data, PAGE_SIZE);
        }
    }
    file_stream.seekp(page_num * PAGE_SIZE);
    file_stream.write((char*)data, PAGE_SIZE);
    file_stream.flush();
    // Track file growth so re-reads after eviction find the data
    uint32_t write_end = (page_num + 1) * PAGE_SIZE;
    if (write_end > file_length) file_length = write_end;
}

// --- LRU Eviction ---

void Pager::evict_lru() {
    // Walk from LRU end toward MRU, skip pinned pages
    for (auto it = lru_order.end(); it != lru_order.begin(); ) {
        --it;
        uint32_t candidate = *it;
        if (pin_counts.count(candidate) == 0) {
            lru_order.erase(it);
            lru_map.erase(candidate);
            flush(candidate);
            std::free(pool[candidate]);
            pool.erase(candidate);
            stat_evicts++;
            return;
        }
    }
    std::cerr << "ERROR: Buffer pool exhausted — all " << pool.size() << " pages are pinned!\n";
}

// --- Page Pinning ---

void Pager::pin_page(uint32_t page_num)   { pin_counts[page_num]++; }
void Pager::unpin_page(uint32_t page_num) {
    auto it = pin_counts.find(page_num);
    if (it != pin_counts.end() && --it->second == 0) pin_counts.erase(it);
}
bool Pager::is_pinned(uint32_t page_num) const { return pin_counts.count(page_num) > 0; }

// --- Free List Management ---

uint32_t Pager::get_unused_page_num() {
    // 1. Try to reuse a page from the free list
    if (header.first_free_page != 0) {
        uint32_t reused = header.first_free_page;
        void* page = get_page(reused);

        // Read the "next" pointer stored at offset 6 of the free page
        header.first_free_page = *((uint32_t*)((char*)page + HEADER_SIZE));
        header.free_pages--;

        // Zero the page so the caller gets a clean slate
        std::memset(page, 0, PAGE_SIZE);

        write_header();
        std::cout << "DEBUG: Reused free page " << reused << "\n";
        return reused;
    }

    // 2. No free pages available — grow the file
    uint32_t new_page = header.total_pages;
    header.total_pages++;
    write_header();
    return new_page;
}

void Pager::free_page(uint32_t page_num) {
    if (page_num <= ROOT_PAGE) {
        std::cout << "ERROR: Cannot free the header or root page.\n";
        return;
    }

    void* page = get_page(page_num);

    // Clear the page, mark as free, store next-pointer at offset 6
    // (offset 6 is after the 6-byte common header, avoiding CRC overlap at bytes 2-5)
    std::memset(page, 0, PAGE_SIZE);
    *((uint8_t*)page) = NODE_FREE;
    *((uint32_t*)((char*)page + HEADER_SIZE)) = header.first_free_page;

    // Push this page to the front of the free list
    header.first_free_page = page_num;
    header.free_pages++;
    write_header();
}

// --- Header Persistence ---

void Pager::write_header() {
    void* page0 = get_page(HEADER_PAGE);
    std::memcpy(page0, &header, sizeof(DbHeader));
}

// --- Debug Helpers ---

void Pager::print_stats() {
    std::cout << "=== ForgeDB Stats ===\n";
    std::cout << "Magic:       0x" << std::hex << header.magic << std::dec << "\n";
    std::cout << "Page Size:   " << header.page_size << " bytes\n";
    std::cout << "Total Pages: " << header.total_pages << "\n";
    std::cout << "Free Pages:  " << header.free_pages << "\n";
    std::cout << "Free Head:   " << (header.first_free_page ? std::to_string(header.first_free_page) : "(none)") << "\n";
}

void Pager::print_free_list() {
    uint32_t pg = header.first_free_page;
    std::cout << "Free List: ";
    if (pg == 0) {
        std::cout << "(empty)\n";
        return;
    }
    while (pg != 0) {
        std::cout << "[Page " << pg << "]";
        void* p = get_page(pg);
        pg = *((uint32_t*)((char*)p + HEADER_SIZE));
        if (pg != 0) std::cout << " -> ";
    }
    std::cout << "\n";
}

void Pager::print_pool_stats() {
    std::cout << "=== Buffer Pool ===\n";
    std::cout << "Frames:     " << pool.size() << " / " << BUFFER_POOL_SIZE << "\n";
    std::cout << "Pinned:     " << pin_counts.size() << "\n";
    std::cout << "Cache Hits: " << stat_hits << "\n";
    std::cout << "Misses:     " << stat_misses << "\n";
    std::cout << "Evictions:  " << stat_evicts << "\n";
    if (stat_hits + stat_misses > 0) {
        double ratio = (double)stat_hits / (stat_hits + stat_misses) * 100.0;
        std::printf("Hit Ratio:  %.1f%%\n", ratio);
    }
}
