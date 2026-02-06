#include "btree.h"
#include "utils.h"
#include <iostream>

// ==========================================
// B+ TREE IMPLEMENTATION
// ==========================================

BTree::BTree(Pager& p) : pager(p), root_page_num(ROOT_PAGE) {
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

// ==========================================
// INSERT
// ==========================================

void BTree::insert(uint32_t id, Row& row) {
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

// ==========================================
// DELETE
// ==========================================

bool BTree::remove(uint32_t id) {
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

// ==========================================
// VISUALIZATION
// ==========================================

void BTree::print_tree() {
    _print_tree(root_page_num, 0);
}

void BTree::print_json() {
    _print_json(root_page_num);
    std::cout << "\n";
}

void BTree::select_all() {
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
void BTree::range_scan(uint32_t start, uint32_t end) {
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

uint32_t BTree::get_leftmost_leaf() {
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

// ==========================================
// BLOOM FILTER PUBLIC API
// ==========================================

bool BTree::find_row(uint32_t id, Row& out_row) {
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

void BTree::print_bloom_stats() { bloom.print_stats(); }
void BTree::do_rebuild_bloom() { rebuild_bloom(); }

// ==========================================
// PRIVATE: CURSOR / FIND
// ==========================================

BTree::Cursor BTree::find(uint32_t key) {
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

// ==========================================
// PRIVATE: LEAF SPLIT
// ==========================================

void BTree::split_leaf(Cursor& cursor, uint32_t new_key, Row& new_row) {
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
// PRIVATE: INTERNAL NODE SPLIT (recursive)
// ==========================================

void BTree::split_internal(uint32_t internal_page, uint32_t child_index,
                    uint32_t new_key, uint32_t new_child_page,
                    std::vector<uint32_t>& path) {
    InternalNode old_node(pager.get_page(internal_page));
    uint32_t N = old_node.get_num_keys(); // N == INTERNAL_MAX_CELLS

    // 1. Build temporary arrays for the (N+1) keys and (N+2) children
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
// PRIVATE: DELETE HELPERS
// ==========================================

uint32_t BTree::find_child_index(InternalNode& parent, uint32_t child_page) {
    uint32_t nk = parent.get_num_keys();
    for (uint32_t i = 0; i < nk; i++) {
        if (*parent.get_cell(i) == child_page) return i;
    }
    if (parent.get_right_child() == child_page) return nk;
    std::cout << "CRITICAL ERROR: child not found in parent!\n";
    return UINT32_MAX;
}

// --- Leaf Rebalance ---

void BTree::rebalance_leaf(uint32_t page_num, std::vector<uint32_t>& path) {
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
            uint32_t ln = left_sib.get_num_cells();
            Row borrowed = left_sib.get_row(ln - 1);
            leaf.insert(borrowed.id, borrowed);
            left_sib.remove_at(ln - 1);

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
            Row borrowed = right_sib.get_row(0);
            leaf.insert(borrowed.id, borrowed);
            right_sib.remove_at(0);

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
void BTree::merge_leaves(uint32_t left_page, uint32_t right_page,
                  uint32_t parent_page, uint32_t sep_idx,
                  std::vector<uint32_t>& path) {
    LeafNode left(pager.get_page(left_page));
    LeafNode right(pager.get_page(right_page));

    uint32_t rn = right.get_num_cells();
    for (uint32_t i = 0; i < rn; i++) {
        Row row = right.get_row(i);
        left.insert(row.id, row);
    }

    // Bypass right in the sibling chain
    left.set_next_leaf(right.get_next_leaf());

    pager.free_page(right_page);
    std::cout << "DEBUG: Merged leaf Pages " << left_page << " + " << right_page << " (freed " << right_page << ")\n";

    InternalNode parent(pager.get_page(parent_page));
    parent.remove_key(sep_idx);

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

void BTree::rebalance_internal(uint32_t page_num, std::vector<uint32_t>& path) {
    if (path.empty()) return;

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

            uint32_t ln = left_sib.get_num_keys();
            uint32_t borrowed_child = left_sib.get_right_child();
            uint32_t borrowed_key = left_sib.get_key(ln - 1);
            left_sib.set_right_child(*left_sib.get_cell(ln - 1));
            left_sib.set_num_keys(ln - 1);

            uint32_t cn = current.get_num_keys();
            for (int32_t i = cn - 1; i >= 0; i--) {
                std::memcpy(current.get_cell(i + 1), current.get_cell(i), INTERNAL_CELL_SIZE);
            }
            *current.get_cell(0) = borrowed_child;
            current.set_key(0, parent_key);
            current.set_num_keys(cn + 1);

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

            uint32_t borrowed_child = *right_sib.get_cell(0);
            uint32_t borrowed_key = right_sib.get_key(0);
            uint32_t rn = right_sib.get_num_keys();
            for (uint32_t i = 0; i < rn - 1; i++) {
                std::memcpy(right_sib.get_cell(i), right_sib.get_cell(i + 1), INTERNAL_CELL_SIZE);
            }
            right_sib.set_num_keys(rn - 1);

            uint32_t cn = current.get_num_keys();
            *current.get_cell(cn) = current.get_right_child();
            current.set_key(cn, parent_key);
            current.set_right_child(borrowed_child);
            current.set_num_keys(cn + 1);

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
void BTree::merge_internals(uint32_t left_page, uint32_t right_page,
                     uint32_t parent_page, uint32_t sep_idx,
                     std::vector<uint32_t>& path) {
    InternalNode left(pager.get_page(left_page));
    InternalNode right(pager.get_page(right_page));
    InternalNode parent(pager.get_page(parent_page));

    uint32_t separator = parent.get_key(sep_idx);
    uint32_t ln = left.get_num_keys();
    uint32_t rn = right.get_num_keys();

    // 1. Pull separator down
    *left.get_cell(ln) = left.get_right_child();
    left.set_key(ln, separator);

    // 2. Copy all cells from right into left
    for (uint32_t i = 0; i < rn; i++) {
        std::memcpy(left.get_cell(ln + 1 + i), right.get_cell(i), INTERNAL_CELL_SIZE);
    }

    // 3. Left's new right_child = right's right_child
    left.set_right_child(right.get_right_child());
    left.set_num_keys(ln + 1 + rn);

    pager.free_page(right_page);
    std::cout << "DEBUG: Merged internal Pages " << left_page << " + " << right_page << "\n";

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

// ==========================================
// PRIVATE: TREE PRINTING
// ==========================================

void BTree::_print_tree(uint32_t page_num, uint32_t level) {
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

void BTree::_print_json(uint32_t page_num) {
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
void BTree::rebuild_bloom() {
    bloom.clear();
    uint32_t curr = get_leftmost_leaf();
    while (curr != 0) {
        LeafNode leaf(pager.get_page(curr));
        for (uint32_t i = 0; i < leaf.get_num_cells(); i++)
            bloom.add(leaf.get_key(i));
        curr = leaf.get_next_leaf();
    }
}
