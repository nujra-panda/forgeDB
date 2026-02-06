#include "node.h"
#include "utils.h"

// ==========================================
// LEAF NODE IMPLEMENTATION
// ==========================================

void LeafNode::initialize() {
    set_type(NODE_LEAF);
    set_root(false);
    set_num_cells(0);
    set_data_end((uint16_t)PAGE_SIZE);
    set_total_free((uint16_t)LEAF_USABLE_SPACE);
    set_next_leaf(0);
}

uint32_t LeafNode::get_key(uint32_t i) const {
    uint32_t key;
    std::memcpy(&key, record_ptr(i), 4);
    return key;
}

Row LeafNode::get_row(uint32_t i) const {
    return deserialize_row(record_ptr(i));
}

bool LeafNode::can_fit(uint16_t record_size) const {
    return get_total_free() >= record_size + SLOT_SIZE;
}

uint16_t LeafNode::contiguous_free() const {
    uint16_t slot_end = LEAF_HEADER_SIZE + get_num_cells() * SLOT_SIZE;
    return get_data_end() - slot_end;
}

bool LeafNode::leaf_underflow() const {
    if (get_num_cells() < LEAF_MIN_CELLS) return true;
    // Also underflow if used bytes < half of usable space
    uint16_t used = LEAF_USABLE_SPACE - get_total_free();
    return used < LEAF_USABLE_SPACE / 2;
}

// Compact records towards end of page, eliminating holes
void LeafNode::defragment() {
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

// Insert in sorted position (binary search)
void LeafNode::insert(uint32_t key, const Row& row) {
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

// Remove by slot index
void LeafNode::remove_at(uint32_t idx) {
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

// Remove by key (binary search)
bool LeafNode::remove(uint32_t key) {
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

// ==========================================
// INTERNAL NODE IMPLEMENTATION
// ==========================================

void InternalNode::initialize() {
    set_type(NODE_INTERNAL);
    set_num_keys(0);
    set_root(false);
}

uint32_t InternalNode::get_child(uint32_t index) {
    if (index == get_num_keys()) return get_right_child();
    return *get_cell(index);
}

void InternalNode::set_child(uint32_t index, uint32_t child_page) {
    if (index == get_num_keys()) {
        set_right_child(child_page);
    } else {
        *get_cell(index) = child_page;
    }
}

uint32_t InternalNode::get_key(uint32_t index) {
    return *(get_cell(index) + 1);
}

void InternalNode::set_key(uint32_t index, uint32_t key) {
    *(get_cell(index) + 1) = key;
}

// Returns the child page where 'key' belongs  (binary search — O(log N))
uint32_t InternalNode::find_child(uint32_t key) {
    uint32_t lo = 0, hi = get_num_keys();
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        if (get_key(mid) <= key) lo = mid + 1;
        else hi = mid;
    }
    return get_child(lo);  // lo == num_keys → right_child via get_child()
}

// Correct B+Tree Internal Node Insertion
void InternalNode::insert_child(uint32_t index, uint32_t key, uint32_t new_child_page) {
    uint32_t num = get_num_keys();

    // 1. Updating Right-Most Child (Simpler case)
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
        for (uint32_t i = num - 1; i > index + 1; i--) {
            std::memcpy(get_cell(i), get_cell(i - 1), INTERNAL_CELL_SIZE);
        }

        // Logic:
        // Parent: ... [Child_i] [Key_Old] [Child_i+1] ...
        // Split Child_i -> Left, Key_New, Right.
        // Result: ... [Child_i(Left)] [Key_New] [Child_New(Right)] [Key_Old] [Child_i+1] ...
        uint32_t key_old = get_key(index);
        set_key(index, key);

        *get_cell(index + 1) = new_child_page;
        set_key(index + 1, key_old);
    }
    set_num_keys(num + 1);
}

// Remove key at key_index and the child to its RIGHT (used after a merge).
void InternalNode::remove_key(uint32_t key_index) {
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
