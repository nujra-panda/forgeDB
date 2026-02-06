#pragma once
#include <cstdint>
#include <cstring>

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

// Common Header Layout  [type:1][is_root:1][crc32:4] = 6 bytes
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
// Each free page stores the next-free page number at offset HEADER_SIZE (byte 6).
// Offset 0 is marked NODE_FREE to prevent CRC stamping on flush.
// A next-pointer value of 0 means "end of list".
struct DbHeader {
    uint32_t magic;            // 0xF04DB for validation
    uint32_t page_size;        // Page size used for this DB
    uint32_t total_pages;      // Total pages allocated (header + data + free)
    uint32_t free_pages;       // Count of pages currently in the free list
    uint32_t first_free_page;  // Head of free page linked list (0 = empty)
};

// Bloom Filter Constants (stored on Page 0 after DbHeader)
const uint32_t BLOOM_OFFSET = sizeof(DbHeader);          // byte 20
const uint32_t BLOOM_SIZE   = PAGE_SIZE - BLOOM_OFFSET;   // 4076 bytes
const uint32_t BLOOM_BITS   = BLOOM_SIZE * 8;             // 32608 bits
