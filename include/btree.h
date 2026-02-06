#pragma once
#include "pager.h"
#include "node.h"
#include "bloom.h"
#include <vector>

// ==========================================
// CLASS: B+ TREE (Logic)
// ==========================================
class BTree {
    Pager& pager;
    uint32_t root_page_num;
    BloomFilter bloom;

    // --- Private helpers ---
    struct Cursor {
        uint32_t page_num;
        std::vector<uint32_t> path_stack;
    };

    Cursor find(uint32_t key);

    void split_leaf(Cursor& cursor, uint32_t new_key, Row& new_row);
    void split_internal(uint32_t internal_page, uint32_t child_index,
                        uint32_t new_key, uint32_t new_child_page,
                        std::vector<uint32_t>& path);

    uint32_t find_child_index(InternalNode& parent, uint32_t child_page);

    void rebalance_leaf(uint32_t page_num, std::vector<uint32_t>& path);
    void merge_leaves(uint32_t left_page, uint32_t right_page,
                      uint32_t parent_page, uint32_t sep_idx,
                      std::vector<uint32_t>& path);

    void rebalance_internal(uint32_t page_num, std::vector<uint32_t>& path);
    void merge_internals(uint32_t left_page, uint32_t right_page,
                         uint32_t parent_page, uint32_t sep_idx,
                         std::vector<uint32_t>& path);

    void _print_tree(uint32_t page_num, uint32_t level);
    void _print_json(uint32_t page_num);

    void rebuild_bloom();

public:
    BTree(Pager& p);

    void insert(uint32_t id, Row& row);
    bool remove(uint32_t id);

    void print_tree();
    void print_json();
    void select_all();
    void range_scan(uint32_t start, uint32_t end);
    uint32_t get_leftmost_leaf();

    // --- Bloom Filter public API ---
    bool find_row(uint32_t id, Row& out_row);
    void print_bloom_stats();
    void do_rebuild_bloom();
};
