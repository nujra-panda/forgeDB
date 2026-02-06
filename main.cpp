#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <string>
#include <cstdint>
#include <memory>
#include <algorithm>
#include <cmath>
#include <list>
#include <unordered_map>

// ==========================================
// CONSTANTS & CONFIGURATION
// ==========================================
const uint32_t PAGE_SIZE = 4096;
const uint32_t BUFFER_POOL_SIZE = 100;  // Max page frames in RAM (LRU eviction beyond this)
                                        // Must be ≥ tree height + max pages touched per operation (~10)

struct Row {
    uint32_t id;
    char username[32];
    char email[255];
};

// Node Types
const uint8_t NODE_INTERNAL = 0;
const uint8_t NODE_LEAF = 1;
const uint8_t NODE_FREE = 2;  // Freed page marker (prevents CRC stamping)

// Header Layout  [type:1][is_root:1][crc32:4] = 6 bytes
// Parent pointers intentionally omitted — stack-based traversal (path_stack)
// avoids expensive recursive parent updates during splits/merges.
const uint32_t OFFSET_TYPE     = 0;
const uint32_t OFFSET_IS_ROOT  = OFFSET_TYPE + 1;
const uint32_t OFFSET_CHECKSUM = OFFSET_IS_ROOT + 1;  // CRC32 page integrity (4 bytes)
const uint32_t HEADER_SIZE     = OFFSET_CHECKSUM + 4;  // 6 bytes common header

// Slotted Leaf Layout (B-Link: leaves form a singly-linked list)
// Header: [type:1][is_root:1][crc32:4][num_cells:4][data_end:2][total_free:2][next_leaf:4] = 18 bytes
// Slot directory grows down (towards higher addresses) from header.
// Each slot: [offset:u16][length:u16] = 4 bytes.  Points to a record.
// Records grow up from the bottom of the page.
const uint32_t OFFSET_LEAF_NUM_CELLS  = HEADER_SIZE;       // uint32_t @ byte 6
const uint32_t OFFSET_LEAF_DATA_END   = HEADER_SIZE + 4;   // uint16_t @ byte 10
const uint32_t OFFSET_LEAF_TOTAL_FREE = HEADER_SIZE + 6;   // uint16_t @ byte 12
const uint32_t OFFSET_LEAF_NEXT       = HEADER_SIZE + 8;   // uint32_t @ byte 14 (→ next leaf)
const uint32_t LEAF_HEADER_SIZE       = HEADER_SIZE + 12;  // 18 bytes total
const uint32_t SLOT_SIZE = 4;  // per-slot overhead
const uint32_t LEAF_USABLE_SPACE = PAGE_SIZE - LEAF_HEADER_SIZE;

// Internal Layout
const uint32_t OFFSET_INTERNAL_NUM_KEYS = HEADER_SIZE;
const uint32_t OFFSET_INTERNAL_RIGHT_CHILD = OFFSET_INTERNAL_NUM_KEYS + 4;
const uint32_t INTERNAL_HEADER_SIZE = OFFSET_INTERNAL_RIGHT_CHILD + 4;
const uint32_t INTERNAL_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_CELL_SIZE = INTERNAL_CHILD_SIZE + INTERNAL_KEY_SIZE; 
const uint32_t INTERNAL_MAX_CELLS = (PAGE_SIZE - INTERNAL_HEADER_SIZE) / INTERNAL_CELL_SIZE;

// Minimum occupancy thresholds (for delete / rebalance)
// With variable-length records, leaf underflow is byte-based:
//   underflow when used_bytes < LEAF_USABLE_SPACE / 2
// We also keep a hard floor: a leaf with < 2 cells always rebalances.
const uint32_t LEAF_MIN_CELLS = 2;   // absolute floor
const uint32_t INTERNAL_MIN_KEYS = INTERNAL_MAX_CELLS / 2;

// ==========================================
// DB FILE HEADER (Stored in Page 0)
// ==========================================
const uint32_t DB_MAGIC = 0xF04DB;
const uint32_t HEADER_PAGE = 0;
const uint32_t ROOT_PAGE = 1;

// Free pages form a singly linked list.
// Each free page stores the next-free page number at byte offset 0.
// A value of 0 means "end of list".
struct DbHeader {
    uint32_t magic;            // 0xF04DB for validation
    uint32_t page_size;        // Page size used for this DB
    uint32_t total_pages;      // Total pages allocated (header + data + free)
    uint32_t free_pages;       // Count of pages currently in the free list
    uint32_t first_free_page;  // Head of free page linked list (0 = empty)
};

// ==========================================
// VARIABLE-LENGTH ROW SERIALIZATION
// ==========================================
// Wire format: [id:4B][username_len:2B][username:NB][email_len:2B][email:MB]
// Min size: 4+2+0+2+0 = 8 bytes   Max size: 4+2+31+2+254 = 293 bytes

uint16_t serialize_row(const Row& row, uint8_t* dest) {
    uint16_t off = 0;
    std::memcpy(dest + off, &row.id, 4);   off += 4;
    uint16_t ulen = (uint16_t)std::strlen(row.username);
    std::memcpy(dest + off, &ulen, 2);     off += 2;
    std::memcpy(dest + off, row.username, ulen);  off += ulen;
    uint16_t elen = (uint16_t)std::strlen(row.email);
    std::memcpy(dest + off, &elen, 2);     off += 2;
    std::memcpy(dest + off, row.email, elen);     off += elen;
    return off;
}

Row deserialize_row(const uint8_t* src) {
    Row row;
    std::memset(&row, 0, sizeof(Row));
    uint16_t off = 0;
    std::memcpy(&row.id, src + off, 4);   off += 4;
    uint16_t ulen;
    std::memcpy(&ulen, src + off, 2);     off += 2;
    std::memcpy(row.username, src + off, ulen);
    row.username[ulen] = '\0';            off += ulen;
    uint16_t elen;
    std::memcpy(&elen, src + off, 2);     off += 2;
    std::memcpy(row.email, src + off, elen);
    row.email[elen] = '\0';
    return row;
}

uint16_t serialized_row_size(const Row& row) {
    return 4 + 2 + (uint16_t)std::strlen(row.username) + 2 + (uint16_t)std::strlen(row.email);
}

// ==========================================
// CRC32 PAGE CHECKSUMS
// ==========================================
// Standard CRC32 (ISO 3309) with 0xEDB88320 polynomial.
// Computed over the full 4096-byte page with the checksum field zeroed.

static uint32_t crc32_table[256];
static bool     crc32_table_ready = false;

static void crc32_init() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_ready = true;
}

static uint32_t crc32_compute(const uint8_t* buf, uint32_t len) {
    if (!crc32_table_ready) crc32_init();
    uint32_t crc = 0xFFFFFFFF;
    for (uint32_t i = 0; i < len; i++)
        crc = crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFF;
}

// ==========================================
// CLASS: BLOOM FILTER (Probabilistic Index)
// ==========================================
// Stored on Page 0 after the DbHeader.  Persisted automatically via page cache.
// On startup, rebuilt by scanning all leaf nodes (handles migration + stale bits
// from deletes).  3 hash functions → very low false-positive rate at ForgeDB scale.
//
// Layout of Page 0:
//   [DbHeader: 20 bytes][Bloom bit-array: 4076 bytes (32 608 bits)]

const uint32_t BLOOM_OFFSET = sizeof(DbHeader);          // byte 20
const uint32_t BLOOM_SIZE   = PAGE_SIZE - BLOOM_OFFSET;   // 4076 bytes
const uint32_t BLOOM_BITS   = BLOOM_SIZE * 8;             // 32608 bits

class BloomFilter {
    uint8_t* bits;

    // Three independent hash functions (multiplicative hashing with distinct primes)
    uint32_t hash1(uint32_t k) const { return (uint32_t)(((uint64_t)k * 2654435761ULL) % BLOOM_BITS); }
    uint32_t hash2(uint32_t k) const { return (uint32_t)(((uint64_t)k * 0x85ebca6bULL) % BLOOM_BITS); }
    uint32_t hash3(uint32_t k) const { return (uint32_t)(((uint64_t)(k ^ (k >> 16)) * 0xcc9e2d51ULL) % BLOOM_BITS); }

    void set_bit(uint32_t pos) { bits[pos / 8] |= (1 << (pos % 8)); }
    bool get_bit(uint32_t pos) const { return bits[pos / 8] & (1 << (pos % 8)); }

public:
    BloomFilter() : bits(nullptr) {}

    // Point the filter at the bloom area on page 0
    void attach(void* page0) { bits = (uint8_t*)page0 + BLOOM_OFFSET; }

    void add(uint32_t key) {
        set_bit(hash1(key));
        set_bit(hash2(key));
        set_bit(hash3(key));
    }

    // Returns TRUE  → "maybe present"  (must verify in B+Tree)
    // Returns FALSE → "definitely not present"  (skip B+Tree entirely)
    bool possibly_contains(uint32_t key) const {
        return get_bit(hash1(key)) && get_bit(hash2(key)) && get_bit(hash3(key));
    }

    void clear() { std::memset(bits, 0, BLOOM_SIZE); }

    void print_stats() const {
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
};

// ==========================================
// CLASS: PAGER (Disk Manager)
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

    Pager(std::string filename) {
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

    ~Pager() {
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

    void* get_page(uint32_t page_num) {
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

    void flush(uint32_t page_num) {
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
    void evict_lru() {
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

    // --- Page Pinning (prevents eviction) ---
    void pin_page(uint32_t page_num)   { pin_counts[page_num]++; }
    void unpin_page(uint32_t page_num) {
        auto it = pin_counts.find(page_num);
        if (it != pin_counts.end() && --it->second == 0) pin_counts.erase(it);
    }
    bool is_pinned(uint32_t page_num) const { return pin_counts.count(page_num) > 0; }

    // --- Free List Management ---

    uint32_t get_unused_page_num() {
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

    void free_page(uint32_t page_num) {
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

    void write_header() {
        void* page0 = get_page(HEADER_PAGE);
        std::memcpy(page0, &header, sizeof(DbHeader));
    }

    // --- Debug Helpers ---

    void print_stats() {
        std::cout << "=== ForgeDB Stats ===\n";
        std::cout << "Magic:       0x" << std::hex << header.magic << std::dec << "\n";
        std::cout << "Page Size:   " << header.page_size << " bytes\n";
        std::cout << "Total Pages: " << header.total_pages << "\n";
        std::cout << "Free Pages:  " << header.free_pages << "\n";
        std::cout << "Free Head:   " << (header.first_free_page ? std::to_string(header.first_free_page) : "(none)") << "\n";
    }

    void print_free_list() {
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

    void print_pool_stats() {
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
};

// ==========================================
// CLASS: NODE (Memory Abstraction)
// ==========================================
// Handles reading/writing raw bytes within a page
class Node {
protected:
    void* data;
public:
    Node(void* page_data) : data(page_data) {}

    uint8_t get_type() const { return *((uint8_t*)((char*)data + OFFSET_TYPE)); }
    void set_type(uint8_t type) { *((uint8_t*)((char*)data + OFFSET_TYPE)) = type; }

    bool is_root() const { return *((uint8_t*)((char*)data + OFFSET_IS_ROOT)); }
    void set_root(bool is_root) { *((uint8_t*)((char*)data + OFFSET_IS_ROOT)) = is_root; }

    uint32_t get_checksum() const { return *((uint32_t*)((char*)data + OFFSET_CHECKSUM)); }
    void set_checksum(uint32_t crc) { *((uint32_t*)((char*)data + OFFSET_CHECKSUM)) = crc; }
};

class LeafNode : public Node {
public:
    LeafNode(void* data) : Node(data) {}

    void initialize() {
        set_type(NODE_LEAF);
        set_root(false);
        set_num_cells(0);
        set_data_end((uint16_t)PAGE_SIZE);
        set_total_free((uint16_t)LEAF_USABLE_SPACE);
        set_next_leaf(0);
    }

    // --- Header accessors ---
    uint32_t get_num_cells() const { return *((uint32_t*)((char*)data + OFFSET_LEAF_NUM_CELLS)); }
    void set_num_cells(uint32_t num) { *((uint32_t*)((char*)data + OFFSET_LEAF_NUM_CELLS)) = num; }

    uint16_t get_data_end() const { return *((uint16_t*)((char*)data + OFFSET_LEAF_DATA_END)); }
    void set_data_end(uint16_t v) { *((uint16_t*)((char*)data + OFFSET_LEAF_DATA_END)) = v; }

    uint16_t get_total_free() const { return *((uint16_t*)((char*)data + OFFSET_LEAF_TOTAL_FREE)); }
    void set_total_free(uint16_t v) { *((uint16_t*)((char*)data + OFFSET_LEAF_TOTAL_FREE)) = v; }

    // --- Sibling pointer (B-Link) ---
    uint32_t get_next_leaf() const { return *((uint32_t*)((char*)data + OFFSET_LEAF_NEXT)); }
    void set_next_leaf(uint32_t pg) { *((uint32_t*)((char*)data + OFFSET_LEAF_NEXT)) = pg; }

    // --- Slot directory ---
    // Each slot: [offset:u16][length:u16] located at LEAF_HEADER_SIZE + i*SLOT_SIZE
    uint16_t slot_offset(uint32_t i) const {
        return *((uint16_t*)((char*)data + LEAF_HEADER_SIZE + i * SLOT_SIZE));
    }
    void set_slot_offset(uint32_t i, uint16_t v) {
        *((uint16_t*)((char*)data + LEAF_HEADER_SIZE + i * SLOT_SIZE)) = v;
    }
    uint16_t slot_length(uint32_t i) const {
        return *((uint16_t*)((char*)data + LEAF_HEADER_SIZE + i * SLOT_SIZE + 2));
    }
    void set_slot_length(uint32_t i, uint16_t v) {
        *((uint16_t*)((char*)data + LEAF_HEADER_SIZE + i * SLOT_SIZE + 2)) = v;
    }

    // --- Record access ---
    uint8_t* record_ptr(uint32_t i) { return (uint8_t*)data + slot_offset(i); }
    const uint8_t* record_ptr(uint32_t i) const { return (const uint8_t*)data + slot_offset(i); }

    uint32_t get_key(uint32_t i) const {
        uint32_t key;
        std::memcpy(&key, record_ptr(i), 4);
        return key;
    }

    Row get_row(uint32_t i) const { return deserialize_row(record_ptr(i)); }

    // --- Space management ---
    bool can_fit(uint16_t record_size) const {
        return get_total_free() >= record_size + SLOT_SIZE;
    }

    // Contiguous gap between end of slot array and start of lowest record
    uint16_t contiguous_free() const {
        uint16_t slot_end = LEAF_HEADER_SIZE + get_num_cells() * SLOT_SIZE;
        return get_data_end() - slot_end;
    }

    bool leaf_underflow() const {
        if (get_num_cells() < LEAF_MIN_CELLS) return true;
        // Also underflow if used bytes < half of usable space
        uint16_t used = LEAF_USABLE_SPACE - get_total_free();
        return used < LEAF_USABLE_SPACE / 2;
    }

    // Compact records towards end of page, eliminating holes
    void defragment() {
        uint32_t n = get_num_cells();
        if (n == 0) return;
        uint8_t tmp[PAGE_SIZE];
        uint16_t new_end = PAGE_SIZE;
        for (uint32_t i = 0; i < n; i++) {
            uint16_t len = slot_length(i);
            new_end -= len;
            std::memcpy(tmp + new_end, record_ptr(i), len);
            set_slot_offset(i, new_end);
        }
        std::memcpy((char*)data + new_end, tmp + new_end, PAGE_SIZE - new_end);
        set_data_end(new_end);
    }

    // --- Insert in sorted position ---
    void insert(uint32_t key, const Row& row) {
        uint32_t n = get_num_cells();
        uint8_t buf[512];
        uint16_t rec_size = serialize_row(row, buf);

        // Binary search for sorted insert position (upper_bound)
        uint32_t lo = 0, hi = n;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (get_key(mid) < key) lo = mid + 1;
            else hi = mid;
        }
        uint32_t idx = lo;

        // Ensure contiguous space (defrag if needed)
        if (contiguous_free() < rec_size + SLOT_SIZE) {
            defragment();
        }

        // Write record at data_end - rec_size
        uint16_t new_end = get_data_end() - rec_size;
        std::memcpy((char*)data + new_end, buf, rec_size);
        set_data_end(new_end);

        // Shift slot entries right to open slot at idx
        for (uint32_t i = n; i > idx; i--) {
            set_slot_offset(i, slot_offset(i - 1));
            set_slot_length(i, slot_length(i - 1));
        }

        // Write new slot
        set_slot_offset(idx, new_end);
        set_slot_length(idx, rec_size);

        set_num_cells(n + 1);
        set_total_free(get_total_free() - rec_size - SLOT_SIZE);
    }

    // --- Remove by slot index ---
    void remove_at(uint32_t idx) {
        uint32_t n = get_num_cells();
        uint16_t freed = slot_length(idx);

        // Shift slot entries left
        for (uint32_t i = idx; i < n - 1; i++) {
            set_slot_offset(i, slot_offset(i + 1));
            set_slot_length(i, slot_length(i + 1));
        }

        set_num_cells(n - 1);
        set_total_free(get_total_free() + freed + SLOT_SIZE);
        // Record data stays as a hole until defragment()
    }

    // --- Remove by key (binary search) ---
    bool remove(uint32_t key) {
        uint32_t n = get_num_cells();
        uint32_t lo = 0, hi = n;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            uint32_t mk = get_key(mid);
            if (mk < key) lo = mid + 1;
            else if (mk > key) hi = mid;
            else { remove_at(mid); return true; }
        }
        return false;
    }
};

class InternalNode : public Node {
public:
    InternalNode(void* data) : Node(data) {}

    void initialize() {
        set_type(NODE_INTERNAL);
        set_num_keys(0);
        set_root(false);
    }

    uint32_t get_num_keys() const { return *((uint32_t*)((char*)data + OFFSET_INTERNAL_NUM_KEYS)); }
    void set_num_keys(uint32_t num) { *((uint32_t*)((char*)data + OFFSET_INTERNAL_NUM_KEYS)) = num; }

    uint32_t get_right_child() const { return *((uint32_t*)((char*)data + OFFSET_INTERNAL_RIGHT_CHILD)); }
    void set_right_child(uint32_t child) { *((uint32_t*)((char*)data + OFFSET_INTERNAL_RIGHT_CHILD)) = child; }

    // Cell = [Child Ptr] [Key]
    uint32_t* get_cell(uint32_t index) {
        return (uint32_t*)((char*)data + INTERNAL_HEADER_SIZE + (index * INTERNAL_CELL_SIZE));
    }

    uint32_t get_child(uint32_t index) {
        if (index == get_num_keys()) return get_right_child();
        return *get_cell(index);
    }
    
    void set_child(uint32_t index, uint32_t child_page) {
        if (index == get_num_keys()) {
            set_right_child(child_page);
        } else {
            *get_cell(index) = child_page;
        }
    }

    uint32_t get_key(uint32_t index) {
        return *(get_cell(index) + 1);
    }

    void set_key(uint32_t index, uint32_t key) {
        *(get_cell(index) + 1) = key;
    }

    // Returns the child page where 'key' belongs  (binary search — O(log N))
    uint32_t find_child(uint32_t key) {
        uint32_t lo = 0, hi = get_num_keys();
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            if (get_key(mid) <= key) lo = mid + 1;
            else hi = mid;
        }
        return get_child(lo);  // lo == num_keys → right_child via get_child()
    }
    
    // Correct B+Tree Internal Node Insertion
    void insert_child(uint32_t index, uint32_t key, uint32_t new_child_page) {
        uint32_t num = get_num_keys();

        // 1. Updating Right-Most Child (Simpler case)
        // This usually happens when splitting the right-most child.
        if (index == num) {
            *get_cell(num) = get_right_child();
            set_key(num, key);
            set_right_child(new_child_page);
        } 
        // 2. Middle Insertion
        else {
            // Move Right Child to end cell
            *get_cell(num) = get_right_child();
            set_key(num, get_key(num - 1));

            // Shift everything right to make space at index+1
            // We want to insert AFTER 'index'.
            // So we free up cell[index+1].
            for (uint32_t i = num - 1; i > index + 1; i--) {
                std::memcpy(get_cell(i), get_cell(i - 1), INTERNAL_CELL_SIZE);
            }
            
            // At this point, Cell[index+1] is a copy of Cell[index].
            // We need to inject the new child and key relations.
            
            // The old 'index' key is pushed to the right to be the separator for the new child.
            // The NEW key becomes the separator for the left child.
            
            // Logic:
            // Parent: ... [Child_i] [Key_Old] [Child_i+1] ...
            // Split Child_i -> Left, Key_New, Right.
            // Result: ... [Child_i(Left)] [Key_New] [Child_New(Right)] [Key_Old] [Child_i+1] ...
            
            // So:
            // Cell[index] (Child_i): Ptr unchanged. Key changes to Key_New.
            // Cell[index+1]: Ptr becomes Child_New. Key becomes Key_Old.
            
            uint32_t key_old = get_key(index);
            set_key(index, key);
            
            *get_cell(index + 1) = new_child_page;
            set_key(index + 1, key_old);
        }
        set_num_keys(num + 1);
    }

    // Remove key at key_index and the child to its RIGHT (used after a merge).
    // The merged data lives in the child to the LEFT of the key (preserved).
    void remove_key(uint32_t key_index) {
        uint32_t num = get_num_keys();

        if (key_index == num - 1) {
            // Removing last key: left child becomes the new right_child
            set_right_child(*get_cell(key_index));
            set_num_keys(num - 1);
            return;
        }

        // General: save the left child (merged node), shift cells left, restore it
        uint32_t merged_child = *get_cell(key_index);
        for (uint32_t i = key_index; i < num - 1; i++) {
            std::memcpy(get_cell(i), get_cell(i + 1), INTERNAL_CELL_SIZE);
        }
        *get_cell(key_index) = merged_child;
        set_num_keys(num - 1);
    }
};

// ==========================================
// CLASS: B+ TREE (Logic)
// ==========================================
class BTree {
    Pager& pager;
    uint32_t root_page_num;
    BloomFilter bloom;

public:
    BTree(Pager& p) : pager(p), root_page_num(ROOT_PAGE) {
        if (pager.header.total_pages <= ROOT_PAGE) {
            // New DB — create root leaf at page 1
            pager.header.total_pages = ROOT_PAGE + 1;
            void* root = pager.get_page(ROOT_PAGE);
            LeafNode node(root);
            node.initialize();
            node.set_root(true);
            pager.write_header();
        }
        // Attach bloom filter to page 0 and rebuild from leaf scan
        bloom.attach(pager.get_page(HEADER_PAGE));
        rebuild_bloom();
    }

    void insert(uint32_t id, Row& row) {
        Cursor cursor = find(id);
        LeafNode leaf(pager.get_page(cursor.page_num));

        // Duplicate key check — primary keys must be unique
        uint32_t n = leaf.get_num_cells();
        // Binary search for exact match
        uint32_t lo = 0, hi = n;
        while (lo < hi) {
            uint32_t mid = (lo + hi) / 2;
            uint32_t mk = leaf.get_key(mid);
            if (mk < id) lo = mid + 1;
            else if (mk > id) hi = mid;
            else { // exact match
                std::cout << "Error: Duplicate key " << id << "\n";
                return;
            }
        }

        bloom.add(id);
        uint16_t needed = serialized_row_size(row);
        if (!leaf.can_fit(needed)) {
            split_leaf(cursor, id, row);
        } else {
            leaf.insert(id, row);
            std::cout << "Executed. (Inserted into Page " << cursor.page_num
                      << ", record " << needed << "B)\n";
        }
    }

    bool remove(uint32_t id) {
        // Bloom filter: skip tree traversal if key definitely not present
        if (!bloom.possibly_contains(id)) {
            std::cout << "Error: Key " << id << " not found. (bloom: definite negative)\n";
            return false;
        }
        Cursor cursor = find(id);
        void* leaf_raw = pager.get_page(cursor.page_num);
        LeafNode leaf(leaf_raw);

        if (!leaf.remove(id)) {
            std::cout << "Error: Key " << id << " not found.\n";
            return false;
        }

        std::cout << "Deleted key " << id << " from Page " << cursor.page_num << ".\n";

        // Root leaf has no minimum occupancy constraint
        if (leaf.is_root()) return true;

        // No underflow? Done.
        if (!leaf.leaf_underflow()) return true;

        // Leaf underflow — must rebalance
        rebalance_leaf(cursor.page_num, cursor.path_stack);
        return true;
    }

    // Visualization
    void print_tree() {
        _print_tree(root_page_num, 0);
    }

    void print_json() {
        _print_json(root_page_num);
        std::cout << "\n";
    }

    void select_all() {
        uint32_t curr = get_leftmost_leaf();
        while (curr != 0) {
            LeafNode leaf(pager.get_page(curr));
            for (uint32_t i = 0; i < leaf.get_num_cells(); i++) {
                Row row = leaf.get_row(i);
                std::cout << "  (" << row.id << ", " << row.username << ", " << row.email << ")\n";
            }
            curr = leaf.get_next_leaf();
        }
    }

    // Range scan: prints all rows with start ≤ id ≤ end
    void range_scan(uint32_t start, uint32_t end) {
        Cursor cursor = find(start);
        uint32_t curr = cursor.page_num;
        while (curr != 0) {
            LeafNode leaf(pager.get_page(curr));
            for (uint32_t i = 0; i < leaf.get_num_cells(); i++) {
                uint32_t key = leaf.get_key(i);
                if (key > end) return;
                if (key >= start) {
                    Row row = leaf.get_row(i);
                    std::cout << "  (" << row.id << ", " << row.username << ", " << row.email << ")\n";
                }
            }
            curr = leaf.get_next_leaf();
        }
    }

    uint32_t get_leftmost_leaf() {
        uint32_t curr = root_page_num;
        void* raw = pager.get_page(curr);
        Node node(raw);
        while (node.get_type() == NODE_INTERNAL) {
            InternalNode internal(raw);
            curr = internal.get_child(0);
            raw = pager.get_page(curr);
            node = Node(raw);
        }
        return curr;
    }

    // --- Bloom Filter public API ---

    bool find_row(uint32_t id, Row& out_row) {
        if (!bloom.possibly_contains(id)) {
            std::cout << "Bloom: DEFINITELY NOT PRESENT (0 disk reads)\n";
            return false;
        }
        std::cout << "Bloom: MAYBE (searching B+Tree...)\n";
        Cursor cursor = find(id);
        LeafNode leaf(pager.get_page(cursor.page_num));
        for (uint32_t i = 0; i < leaf.get_num_cells(); i++) {
            if (leaf.get_key(i) == id) {
                out_row = leaf.get_row(i);
                return true;
            }
        }
        std::cout << "Bloom: FALSE POSITIVE — key not in B+Tree.\n";
        return false;
    }

    void print_bloom_stats() { bloom.print_stats(); }
    void do_rebuild_bloom() { rebuild_bloom(); }

private:
    struct Cursor {
        uint32_t page_num;
        std::vector<uint32_t> path_stack;
    };

    Cursor find(uint32_t key) {
        uint32_t curr_page = root_page_num;
        std::vector<uint32_t> path;
        
        void* node_raw = pager.get_page(curr_page);
        Node node(node_raw);

        while (node.get_type() == NODE_INTERNAL) {
            path.push_back(curr_page); // Push internal node to stack
            InternalNode internal(node_raw);
            curr_page = internal.find_child(key);
            node_raw = pager.get_page(curr_page);
            node = Node(node_raw);
        }
        return {curr_page, path};
    }

    void split_leaf(Cursor& cursor, uint32_t new_key, Row& new_row) {
        uint32_t page_num = cursor.page_num;
        void* old_node_raw = pager.get_page(page_num);
        LeafNode old_node(old_node_raw);

        // 1. Collect all rows (existing + new) in sorted order
        uint32_t total = old_node.get_num_cells();
        std::vector<Row> all_rows;
        all_rows.reserve(total + 1);
        bool inserted = false;
        for (uint32_t i = 0; i < total; i++) {
            if (!inserted && new_key < old_node.get_key(i)) {
                all_rows.push_back(new_row);
                inserted = true;
            }
            all_rows.push_back(old_node.get_row(i));
        }
        if (!inserted) all_rows.push_back(new_row);

        // 2. Find split point by bytes: try to balance data across both pages
        uint32_t half_usable = LEAF_USABLE_SPACE / 2;
        uint32_t running = 0;
        uint32_t split_point = 0;
        for (uint32_t i = 0; i < all_rows.size(); i++) {
            running += serialized_row_size(all_rows[i]) + SLOT_SIZE;
            if (running > half_usable) {
                split_point = (i > 0) ? i : 1;  // at least 1 in left
                break;
            }
        }
        if (split_point == 0) split_point = all_rows.size() / 2;

        // 3. Allocate new page for right half
        uint32_t new_page_num = pager.get_unused_page_num();
        void* new_node_raw = pager.get_page(new_page_num);
        LeafNode new_node(new_node_raw);
        new_node.initialize();

        // 4. Save sibling chain, re-initialise old page (preserve root flag)
        uint32_t old_next = old_node.get_next_leaf();
        bool was_root = old_node.is_root();
        old_node.initialize();
        old_node.set_root(was_root);

        // 5. Distribute rows
        for (uint32_t i = 0; i < split_point; i++)
            old_node.insert(all_rows[i].id, all_rows[i]);
        for (uint32_t i = split_point; i < all_rows.size(); i++)
            new_node.insert(all_rows[i].id, all_rows[i]);

        // 5b. Wire sibling pointers:  old → new → old's-old-next
        old_node.set_next_leaf(new_page_num);
        new_node.set_next_leaf(old_next);

        uint32_t separator = new_node.get_key(0);

        // 6. Parent update logic
        if (was_root) {
            uint32_t left_copy_page = pager.get_unused_page_num();
            void* left_copy = pager.get_page(left_copy_page);
            std::memcpy(left_copy, old_node_raw, PAGE_SIZE);
            LeafNode left_leaf(left_copy);
            left_leaf.set_root(false);

            InternalNode root(old_node_raw);
            root.initialize();
            root.set_root(true);
            root.set_num_keys(1);
            root.set_right_child(new_page_num);
            root.set_child(0, left_copy_page);
            root.set_key(0, separator);

            std::cout << "DEBUG: Root Split. Left(" << left_copy_page
                      << ") Key(" << separator << ") Right(" << new_page_num << ")\n";
        } else {
            uint32_t parent_page = cursor.path_stack.back();
            InternalNode parent(pager.get_page(parent_page));
            uint32_t child_index = find_child_index(parent, page_num);

            if (parent.get_num_keys() >= INTERNAL_MAX_CELLS) {
                cursor.path_stack.pop_back();
                split_internal(parent_page, child_index,
                               separator, new_page_num,
                               cursor.path_stack);
            } else {
                parent.insert_child(child_index, separator, new_page_num);
                std::cout << "DEBUG: Internal Update. Added child " << new_page_num
                          << " at index " << child_index << "\n";
            }
        }
    }

    // ==========================================
    // INTERNAL NODE SPLIT  (recursive growth)
    // ==========================================
    // Called when an internal node is full and a new key+child must be inserted.
    // Splits the node, pushes the middle key up to the grandparent (or creates a
    // new root).  Recurses if the grandparent is also full.

    void split_internal(uint32_t internal_page, uint32_t child_index,
                        uint32_t new_key, uint32_t new_child_page,
                        std::vector<uint32_t>& path) {
        InternalNode old_node(pager.get_page(internal_page));
        uint32_t N = old_node.get_num_keys(); // N == INTERNAL_MAX_CELLS

        // 1. Build temporary arrays for the (N+1) keys and (N+2) children
        //    that would exist AFTER inserting the new key+child.
        //
        //    Original layout (N keys, N+1 children):
        //      C0 K0  C1 K1  ...  C_{N-1} K_{N-1}  right_child
        //
        //    After inserting new_key at child_index the conceptual layout is:
        //      ... C_ci  new_key  new_child  C_{ci+1} ...
        //    giving (N+1) keys and (N+2) children.

        uint32_t total_keys = N + 1;
        std::vector<uint32_t> keys(total_keys);
        std::vector<uint32_t> children(total_keys + 1);

        // Children
        for (uint32_t i = 0; i <= child_index; i++)
            children[i] = old_node.get_child(i);
        children[child_index + 1] = new_child_page;
        for (uint32_t i = child_index + 1; i <= N; i++)
            children[i + 1] = old_node.get_child(i);

        // Keys
        for (uint32_t i = 0; i < child_index; i++)
            keys[i] = old_node.get_key(i);
        keys[child_index] = new_key;
        for (uint32_t i = child_index; i < N; i++)
            keys[i + 1] = old_node.get_key(i);

        // 2. Split point — middle key is pushed UP, not kept in either node.
        uint32_t mid = total_keys / 2;
        uint32_t push_up_key = keys[mid];

        // 3. Write left half back into old_node.
        uint32_t left_count = mid;
        for (uint32_t i = 0; i < left_count; i++) {
            *old_node.get_cell(i) = children[i];
            old_node.set_key(i, keys[i]);
        }
        old_node.set_right_child(children[mid]);
        old_node.set_num_keys(left_count);

        // 4. Create new internal node for the right half.
        uint32_t new_internal_page = pager.get_unused_page_num();
        InternalNode new_node(pager.get_page(new_internal_page));
        new_node.initialize();

        uint32_t right_count = total_keys - mid - 1;
        for (uint32_t i = 0; i < right_count; i++) {
            *new_node.get_cell(i) = children[mid + 1 + i];
            new_node.set_key(i, keys[mid + 1 + i]);
        }
        new_node.set_right_child(children[total_keys]);
        new_node.set_num_keys(right_count);

        // 5. Push middle key up.
        if (old_node.is_root()) {
            // Copy left half to a fresh page, then re-init the root page.
            uint32_t left_page = pager.get_unused_page_num();
            std::memcpy(pager.get_page(left_page),
                        pager.get_page(internal_page), PAGE_SIZE);
            InternalNode left_copy(pager.get_page(left_page));
            left_copy.set_root(false);

            InternalNode root(pager.get_page(internal_page));
            root.initialize();
            root.set_root(true);
            root.set_num_keys(1);
            root.set_child(0, left_page);
            root.set_key(0, push_up_key);
            root.set_right_child(new_internal_page);

            std::cout << "DEBUG: Internal Root Split. Left(" << left_page
                      << ") Key(" << push_up_key
                      << ") Right(" << new_internal_page << ")\n";
        } else {
            // Recursive: push up to grandparent
            uint32_t parent_page = path.back();
            path.pop_back();
            InternalNode parent(pager.get_page(parent_page));
            uint32_t pidx = find_child_index(parent, internal_page);

            if (parent.get_num_keys() >= INTERNAL_MAX_CELLS) {
                split_internal(parent_page, pidx,
                               push_up_key, new_internal_page, path);
            } else {
                parent.insert_child(pidx, push_up_key, new_internal_page);
                std::cout << "DEBUG: Internal Update (post internal split). Key("
                          << push_up_key << ") -> Page " << parent_page << "\n";
            }
        }
    }

    // ==========================================
    // DELETE HELPERS
    // ==========================================

    // Find which child slot a page occupies in its parent.
    // Returns 0..num_keys-1 for cell children, num_keys for right_child.
    uint32_t find_child_index(InternalNode& parent, uint32_t child_page) {
        uint32_t nk = parent.get_num_keys();
        for (uint32_t i = 0; i < nk; i++) {
            if (*parent.get_cell(i) == child_page) return i;
        }
        if (parent.get_right_child() == child_page) return nk;
        std::cout << "CRITICAL ERROR: child not found in parent!\n";
        return UINT32_MAX;
    }

    // --- Leaf Rebalance ---

    void rebalance_leaf(uint32_t page_num, std::vector<uint32_t>& path) {
        uint32_t parent_page = path.back();
        InternalNode parent(pager.get_page(parent_page));
        LeafNode leaf(pager.get_page(page_num));

        uint32_t child_index = find_child_index(parent, page_num);
        uint32_t num_keys = parent.get_num_keys();

        // Try borrowing from LEFT sibling
        if (child_index > 0) {
            uint32_t left_page = parent.get_child(child_index - 1);
            LeafNode left_sib(pager.get_page(left_page));

            if (!left_sib.leaf_underflow() && left_sib.get_num_cells() > LEAF_MIN_CELLS) {
                // Borrow rightmost cell from left sibling
                uint32_t ln = left_sib.get_num_cells();
                Row borrowed = left_sib.get_row(ln - 1);
                leaf.insert(borrowed.id, borrowed);
                left_sib.remove_at(ln - 1);

                // Update separator: key between left and current
                parent.set_key(child_index - 1, leaf.get_key(0));
                std::cout << "DEBUG: Leaf borrow-left from Page " << left_page << "\n";
                return;
            }
        }

        // Try borrowing from RIGHT sibling
        if (child_index < num_keys) {
            uint32_t right_page = parent.get_child(child_index + 1);
            LeafNode right_sib(pager.get_page(right_page));

            if (!right_sib.leaf_underflow() && right_sib.get_num_cells() > LEAF_MIN_CELLS) {
                // Borrow leftmost cell from right sibling
                Row borrowed = right_sib.get_row(0);
                leaf.insert(borrowed.id, borrowed);
                right_sib.remove_at(0);

                // Update separator: key between current and right
                parent.set_key(child_index, right_sib.get_key(0));
                std::cout << "DEBUG: Leaf borrow-right from Page " << right_page << "\n";
                return;
            }
        }

        // Cannot borrow — must merge
        if (child_index > 0) {
            uint32_t left_page = parent.get_child(child_index - 1);
            merge_leaves(left_page, page_num, parent_page, child_index - 1, path);
        } else {
            uint32_t right_page = parent.get_child(child_index + 1);
            merge_leaves(page_num, right_page, parent_page, child_index, path);
        }
    }

    // Merge right leaf INTO left leaf, free right, remove separator from parent.
    void merge_leaves(uint32_t left_page, uint32_t right_page,
                      uint32_t parent_page, uint32_t sep_idx,
                      std::vector<uint32_t>& path) {
        LeafNode left(pager.get_page(left_page));
        LeafNode right(pager.get_page(right_page));

        // Copy all records from right into left (one by one via slotted insert)
        uint32_t rn = right.get_num_cells();
        for (uint32_t i = 0; i < rn; i++) {
            Row row = right.get_row(i);
            left.insert(row.id, row);
        }

        // Bypass right in the sibling chain:  left → right.next
        left.set_next_leaf(right.get_next_leaf());

        // Free the right page
        pager.free_page(right_page);
        std::cout << "DEBUG: Merged leaf Pages " << left_page << " + " << right_page << " (freed " << right_page << ")\n";

        // Remove the separator key from parent
        InternalNode parent(pager.get_page(parent_page));
        parent.remove_key(sep_idx);

        // Check if the root has collapsed
        if (parent.is_root() && parent.get_num_keys() == 0) {
            uint32_t only_child = parent.get_right_child();
            std::memcpy(pager.get_page(parent_page), pager.get_page(only_child), PAGE_SIZE);
            Node new_root(pager.get_page(parent_page));
            new_root.set_root(true);
            pager.free_page(only_child);
            std::cout << "DEBUG: Root collapsed. Tree shrunk by one level.\n";
        } else if (!parent.is_root() && parent.get_num_keys() < INTERNAL_MIN_KEYS) {
            path.pop_back();
            rebalance_internal(parent_page, path);
        }
    }

    // --- Internal Node Rebalance ---

    void rebalance_internal(uint32_t page_num, std::vector<uint32_t>& path) {
        if (path.empty()) return; // root — no minimum

        uint32_t parent_page = path.back();
        InternalNode parent(pager.get_page(parent_page));
        InternalNode current(pager.get_page(page_num));

        uint32_t child_index = find_child_index(parent, page_num);
        uint32_t num_keys = parent.get_num_keys();

        // Try borrowing from LEFT sibling
        if (child_index > 0) {
            uint32_t left_page = parent.get_child(child_index - 1);
            InternalNode left_sib(pager.get_page(left_page));

            if (left_sib.get_num_keys() > INTERNAL_MIN_KEYS) {
                uint32_t sep = child_index - 1;
                uint32_t parent_key = parent.get_key(sep);

                // Take rightmost key+child from left sibling
                uint32_t ln = left_sib.get_num_keys();
                uint32_t borrowed_child = left_sib.get_right_child();
                uint32_t borrowed_key = left_sib.get_key(ln - 1);
                left_sib.set_right_child(*left_sib.get_cell(ln - 1));
                left_sib.set_num_keys(ln - 1);

                // Prepend [borrowed_child, parent_key] to current
                uint32_t cn = current.get_num_keys();
                for (int32_t i = cn - 1; i >= 0; i--) {
                    std::memcpy(current.get_cell(i + 1), current.get_cell(i), INTERNAL_CELL_SIZE);
                }
                *current.get_cell(0) = borrowed_child;
                current.set_key(0, parent_key);
                current.set_num_keys(cn + 1);

                // Rotate key up to parent
                parent.set_key(sep, borrowed_key);
                std::cout << "DEBUG: Internal borrow-left from Page " << left_page << "\n";
                return;
            }
        }

        // Try borrowing from RIGHT sibling
        if (child_index < num_keys) {
            uint32_t right_page = parent.get_child(child_index + 1);
            InternalNode right_sib(pager.get_page(right_page));

            if (right_sib.get_num_keys() > INTERNAL_MIN_KEYS) {
                uint32_t sep = child_index;
                uint32_t parent_key = parent.get_key(sep);

                // Take leftmost key+child from right sibling
                uint32_t borrowed_child = *right_sib.get_cell(0);
                uint32_t borrowed_key = right_sib.get_key(0);
                uint32_t rn = right_sib.get_num_keys();
                for (uint32_t i = 0; i < rn - 1; i++) {
                    std::memcpy(right_sib.get_cell(i), right_sib.get_cell(i + 1), INTERNAL_CELL_SIZE);
                }
                right_sib.set_num_keys(rn - 1);

                // Append [current.right_child, parent_key] to current
                uint32_t cn = current.get_num_keys();
                *current.get_cell(cn) = current.get_right_child();
                current.set_key(cn, parent_key);
                current.set_right_child(borrowed_child);
                current.set_num_keys(cn + 1);

                // Rotate key up to parent
                parent.set_key(sep, borrowed_key);
                std::cout << "DEBUG: Internal borrow-right from Page " << right_page << "\n";
                return;
            }
        }

        // Must merge internal nodes
        if (child_index > 0) {
            uint32_t left_page = parent.get_child(child_index - 1);
            merge_internals(left_page, page_num, parent_page, child_index - 1, path);
        } else {
            uint32_t right_page = parent.get_child(child_index + 1);
            merge_internals(page_num, right_page, parent_page, child_index, path);
        }
    }

    // Merge right internal node INTO left, pulling separator down from parent.
    void merge_internals(uint32_t left_page, uint32_t right_page,
                         uint32_t parent_page, uint32_t sep_idx,
                         std::vector<uint32_t>& path) {
        InternalNode left(pager.get_page(left_page));
        InternalNode right(pager.get_page(right_page));
        InternalNode parent(pager.get_page(parent_page));

        uint32_t separator = parent.get_key(sep_idx);
        uint32_t ln = left.get_num_keys();
        uint32_t rn = right.get_num_keys();

        // 1. Pull separator down: append [left.right_child, separator] to left
        *left.get_cell(ln) = left.get_right_child();
        left.set_key(ln, separator);

        // 2. Copy all cells from right into left
        for (uint32_t i = 0; i < rn; i++) {
            std::memcpy(left.get_cell(ln + 1 + i), right.get_cell(i), INTERNAL_CELL_SIZE);
        }

        // 3. Left's new right_child = right's right_child
        left.set_right_child(right.get_right_child());
        left.set_num_keys(ln + 1 + rn);

        // Free right page
        pager.free_page(right_page);
        std::cout << "DEBUG: Merged internal Pages " << left_page << " + " << right_page << "\n";

        // Remove separator from parent
        InternalNode parent2(pager.get_page(parent_page));
        parent2.remove_key(sep_idx);

        if (parent2.is_root() && parent2.get_num_keys() == 0) {
            uint32_t only_child = parent2.get_right_child();
            std::memcpy(pager.get_page(parent_page), pager.get_page(only_child), PAGE_SIZE);
            Node new_root(pager.get_page(parent_page));
            new_root.set_root(true);
            pager.free_page(only_child);
            std::cout << "DEBUG: Root collapsed (internal merge). Tree shrunk by one level.\n";
        } else if (!parent2.is_root() && parent2.get_num_keys() < INTERNAL_MIN_KEYS) {
            path.pop_back();
            rebalance_internal(parent_page, path);
        }
    }

    void _print_tree(uint32_t page_num, uint32_t level) {
        void* node_raw = pager.get_page(page_num);
        Node node(node_raw);
        
        for (uint32_t i = 0; i < level; i++) std::cout << "  ";

        if (node.get_type() == NODE_LEAF) {
            LeafNode leaf(node_raw);
            uint16_t used = LEAF_USABLE_SPACE - leaf.get_total_free();
            std::cout << "- LEAF (Page " << page_num << ") | " << leaf.get_num_cells()
                      << " rows, " << used << "B used | next->" 
                      << (leaf.get_next_leaf() ? std::to_string(leaf.get_next_leaf()) : "nil") << "\n";
            for(uint32_t i=0; i<leaf.get_num_cells(); i++) {
                 for (uint32_t j = 0; j < level+1; j++) std::cout << "  ";
                 std::cout << leaf.get_key(i) << " [" << leaf.slot_length(i) << "B]\n";
            }
        } else {
            InternalNode internal(node_raw);
            std::cout << "- INTERNAL (Page " << page_num << ") | " << internal.get_num_keys() << " keys\n";
            for(uint32_t i=0; i<internal.get_num_keys(); i++) {
                _print_tree(internal.get_child(i), level + 1);
                for (uint32_t j = 0; j < level+1; j++) std::cout << "  ";
                std::cout << "Key: " << internal.get_key(i) << "\n";
            }
            _print_tree(internal.get_right_child(), level + 1);
        }
    }
    
    void _print_json(uint32_t page_num) {
        void* node_raw = pager.get_page(page_num);
        Node node(node_raw);

        if (node.get_type() == NODE_LEAF) {
            LeafNode leaf(node_raw);
            std::cout << "{\"type\": \"leaf\", \"page\": " << page_num << ", \"cells\": [";
            for(uint32_t i=0; i<leaf.get_num_cells(); i++) {
                std::cout << leaf.get_key(i);
                if (i < leaf.get_num_cells() - 1) std::cout << ",";
            }
            std::cout << "]}";
        } else {
            InternalNode internal(node_raw);
            std::cout << "{\"type\": \"internal\", \"page\": " << page_num << ", \"children\": [";
            for(uint32_t i=0; i<internal.get_num_keys(); i++) {
                _print_json(internal.get_child(i));
                std::cout << ",";
            }
            _print_json(internal.get_right_child());
            std::cout << "], \"keys\": [";
             for(uint32_t i=0; i<internal.get_num_keys(); i++) {
                std::cout << internal.get_key(i);
                if (i < internal.get_num_keys() - 1) std::cout << ",";
            }
            std::cout << "]}";
        }
    }

    // --- Bloom Filter rebuild (walks leaf linked list) ---
    void rebuild_bloom() {
        bloom.clear();
        uint32_t curr = get_leftmost_leaf();
        while (curr != 0) {
            LeafNode leaf(pager.get_page(curr));
            for (uint32_t i = 0; i < leaf.get_num_cells(); i++)
                bloom.add(leaf.get_key(i));
            curr = leaf.get_next_leaf();
        }
    }
};

// ==========================================
// HELPER: Handle a single command string
// ==========================================
void handle_command(const std::string& input, BTree& tree, Pager& pager) {
    if (input.substr(0, 6) == "insert") {
        Row row;
        std::memset(&row, 0, sizeof(Row));
        char buf[100];
        std::sscanf(input.c_str(), "%99s %u %31s %254s", buf, &row.id, row.username, row.email);
        tree.insert(row.id, row);
    } else if (input.substr(0, 6) == "delete") {
        uint32_t id = 0;
        char buf[100];
        if (std::sscanf(input.c_str(), "%99s %u", buf, &id) == 2) {
            tree.remove(id);
        } else {
            std::cout << "Usage: delete <id>\n";
        }
    } else if (input == "select") {
        tree.select_all();
    } else if (input.substr(0, 5) == "range") {
        uint32_t start = 0, end = 0;
        char buf[100];
        if (std::sscanf(input.c_str(), "%99s %u %u", buf, &start, &end) == 3) {
            tree.range_scan(start, end);
        } else {
            std::cout << "Usage: range <start_id> <end_id>\n";
        }
    } else if (input.substr(0, 6) == "lookup") {
        uint32_t id = 0;
        char buf[100];
        if (std::sscanf(input.c_str(), "%99s %u", buf, &id) == 2) {
            Row row;
            if (tree.find_row(id, row)) {
                std::cout << "Found: (" << row.id << ", " << row.username << ", " << row.email << ")\n";
            }
        } else {
            std::cout << "Usage: lookup <id>\n";
        }
    } else if (input == ".tree") {
        tree.print_tree();
    } else if (input == ".json") {
        tree.print_json();
    } else if (input == ".stats") {
        pager.print_stats();
    } else if (input == ".pool") {
        pager.print_pool_stats();
    } else if (input == ".freelist") {
        pager.print_free_list();
    } else if (input == ".bloom rebuild") {
        tree.do_rebuild_bloom();
        std::cout << "Bloom filter rebuilt from B+Tree.\n";
    } else if (input == ".bloom") {
        tree.print_bloom_stats();
    } else if (input.substr(0, 6) == ".free ") {
        uint32_t pg = 0;
        if (std::sscanf(input.c_str(), ".free %u", &pg) == 1 && pg > ROOT_PAGE) {
            pager.free_page(pg);
            std::cout << "Freed page " << pg << ".\n";
        } else {
            std::cout << "Usage: .free <page_num>  (page must be > " << ROOT_PAGE << ")\n";
        }
    } else if (input == "exit") {
        // Handled in main loop — break ensures Pager destructor flushes pages
    } else {
        std::cout << "Unrecognized command.\n";
    }
}

// ==========================================
// MAIN DRIVER
// ==========================================
int main(int argc, char* argv[]) {
    Pager pager("my_database.db");
    BTree tree(pager);

    // MODE 1: Script Mode (For Web Visualizer)
    // Usage: ./forgedb "insert 1 alice alice@example.com"
    //        ./forgedb .json
    if (argc > 1) {
        std::string command = argv[1];
        for (int i = 2; i < argc; i++) {
            command += " " + std::string(argv[i]);
        }
        handle_command(command, tree, pager);
        return 0;
    }

    // MODE 2: Interactive Mode (CLI)
    std::cout << "ForgeDB v1.7 (Buffer Pool Edition)\n";
    std::string input;
    while (true) {
        std::cout << "db > " << std::flush;
        if (!std::getline(std::cin, input) || input == "exit") break;
        handle_command(input, tree, pager);
    }
    return 0;
}