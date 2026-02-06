#pragma once
#include "common.h"

// ==========================================
// CLASS: NODE (Memory Abstraction)
// ==========================================
// Handles reading/writing the 6-byte common header within a page.
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

// ==========================================
// CLASS: LEAF NODE (Slotted Page, B-Link)
// ==========================================
class LeafNode : public Node {
public:
    LeafNode(void* data) : Node(data) {}

    void initialize();

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

    uint32_t get_key(uint32_t i) const;
    Row get_row(uint32_t i) const;

    // --- Space management ---
    bool can_fit(uint16_t record_size) const;
    uint16_t contiguous_free() const;
    bool leaf_underflow() const;
    void defragment();

    // --- Modification ---
    void insert(uint32_t key, const Row& row);
    void remove_at(uint32_t idx);
    bool remove(uint32_t key);
};

// ==========================================
// CLASS: INTERNAL NODE
// ==========================================
class InternalNode : public Node {
public:
    InternalNode(void* data) : Node(data) {}

    void initialize();

    uint32_t get_num_keys() const { return *((uint32_t*)((char*)data + OFFSET_INTERNAL_NUM_KEYS)); }
    void set_num_keys(uint32_t num) { *((uint32_t*)((char*)data + OFFSET_INTERNAL_NUM_KEYS)) = num; }

    uint32_t get_right_child() const { return *((uint32_t*)((char*)data + OFFSET_INTERNAL_RIGHT_CHILD)); }
    void set_right_child(uint32_t child) { *((uint32_t*)((char*)data + OFFSET_INTERNAL_RIGHT_CHILD)) = child; }

    // Cell = [Child Ptr : 4B] [Key : 4B]
    uint32_t* get_cell(uint32_t index) {
        return (uint32_t*)((char*)data + INTERNAL_HEADER_SIZE + (index * INTERNAL_CELL_SIZE));
    }

    uint32_t get_child(uint32_t index);
    void set_child(uint32_t index, uint32_t child_page);
    uint32_t get_key(uint32_t index);
    void set_key(uint32_t index, uint32_t key);

    // B+Tree traversal & modification
    uint32_t find_child(uint32_t key);
    void insert_child(uint32_t index, uint32_t key, uint32_t new_child_page);
    void remove_key(uint32_t key_index);
};
