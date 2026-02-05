#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <string>
#include <cstdint>
#include <memory>
#include <algorithm>

// ==========================================
// CONSTANTS & CONFIGURATION
// ==========================================
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;

struct Row {
    uint32_t id;
    char username[32];
    char email[255];
};

const uint32_t ROW_SIZE = sizeof(Row);

// Node Types
const uint8_t NODE_INTERNAL = 0;
const uint8_t NODE_LEAF = 1;

// Header Layout
const uint32_t OFFSET_TYPE = 0;
const uint32_t OFFSET_IS_ROOT = OFFSET_TYPE + 1;
const uint32_t OFFSET_PARENT = OFFSET_IS_ROOT + 1;
const uint32_t HEADER_SIZE = OFFSET_PARENT + 4; // Common header

// Leaf Layout
const uint32_t OFFSET_LEAF_NUM_CELLS = HEADER_SIZE;
const uint32_t LEAF_HEADER_SIZE = OFFSET_LEAF_NUM_CELLS + 4;
const uint32_t LEAF_CELL_SIZE = ROW_SIZE;
const uint32_t LEAF_MAX_CELLS = (PAGE_SIZE - LEAF_HEADER_SIZE) / LEAF_CELL_SIZE;

// Internal Layout
const uint32_t OFFSET_INTERNAL_NUM_KEYS = HEADER_SIZE;
const uint32_t OFFSET_INTERNAL_RIGHT_CHILD = OFFSET_INTERNAL_NUM_KEYS + 4;
const uint32_t INTERNAL_HEADER_SIZE = OFFSET_INTERNAL_RIGHT_CHILD + 4;
const uint32_t INTERNAL_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_CELL_SIZE = INTERNAL_CHILD_SIZE + INTERNAL_KEY_SIZE; 
const uint32_t INTERNAL_MAX_CELLS = (PAGE_SIZE - INTERNAL_HEADER_SIZE) / INTERNAL_CELL_SIZE;

// ==========================================
// CLASS: PAGER (Disk Manager)
// ==========================================
class Pager {
public:
    std::fstream file_stream;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];

    Pager(std::string filename) {
        // Initialize cache
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) pages[i] = nullptr;

        // Open/Create File
        std::ifstream check(filename);
        if (!check.good()) {
            std::ofstream create(filename);
            create.close();
        }
        check.close();

        file_stream.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        file_stream.seekp(0, std::ios::end);
        file_length = file_stream.tellp();
        num_pages = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE) num_pages++;
    }

    ~Pager() {
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
            if (pages[i]) {
                flush(i);
                std::free(pages[i]);
            }
        }
        file_stream.close();
    }

    void* get_page(uint32_t page_num) {
        if (page_num >= TABLE_MAX_PAGES) return nullptr;
        if (pages[page_num]) return pages[page_num];

        void* page = std::calloc(1, PAGE_SIZE);
        uint32_t saved_pages = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE) saved_pages++;

        if (page_num < saved_pages) {
            file_stream.seekg(page_num * PAGE_SIZE);
            file_stream.read((char*)page, PAGE_SIZE);
        }
        // If it's a new page (beyond file length), we just return zeroed memory
        // Updates happen when we flush.
        
        pages[page_num] = page;
        if (page_num >= num_pages) num_pages = page_num + 1;
        
        return page;
    }

    void flush(uint32_t page_num) {
        if (!pages[page_num]) return;
        file_stream.seekp(page_num * PAGE_SIZE);
        file_stream.write((char*)pages[page_num], PAGE_SIZE);
        file_stream.flush();
    }
    
    uint32_t get_unused_page_num() {
        return num_pages;
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

    uint32_t get_parent() const { return *((uint32_t*)((char*)data + OFFSET_PARENT)); }
    void set_parent(uint32_t parent) { *((uint32_t*)((char*)data + OFFSET_PARENT)) = parent; }
};

class LeafNode : public Node {
public:
    LeafNode(void* data) : Node(data) {}

    void initialize() {
        set_type(NODE_LEAF);
        set_num_cells(0);
        set_root(false);
    }

    uint32_t get_num_cells() const { return *((uint32_t*)((char*)data + OFFSET_LEAF_NUM_CELLS)); }
    void set_num_cells(uint32_t num) { *((uint32_t*)((char*)data + OFFSET_LEAF_NUM_CELLS)) = num; }

    Row* get_cell(uint32_t index) {
        return (Row*)((char*)data + LEAF_HEADER_SIZE + (index * LEAF_CELL_SIZE));
    }

    uint32_t get_key(uint32_t index) {
        return get_cell(index)->id;
    }

    // Insert into sorted position
    void insert(uint32_t key, Row* row) {
        uint32_t num = get_num_cells();
        uint32_t idx = 0;
        
        // Find position
        while (idx < num && get_key(idx) < key) idx++;

        // Shift
        for (uint32_t i = num; i > idx; i--) {
            std::memcpy(get_cell(i), get_cell(i-1), LEAF_CELL_SIZE);
        }

        // Write
        std::memcpy(get_cell(idx), row, ROW_SIZE);
        set_num_cells(num + 1);
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

    // Returns the child page index where 'key' belongs
    uint32_t find_child(uint32_t key) {
        uint32_t num = get_num_keys();
        // Simple linear search (Binary search recommended for production)
        for (uint32_t i = 0; i < num; i++) {
            if (key < get_key(i)) return get_child(i);
        }
        return get_right_child();
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
};

// ==========================================
// CLASS: B+ TREE (Logic)
// ==========================================
class BTree {
    Pager& pager;
    uint32_t root_page_num;

public:
    BTree(Pager& p) : pager(p), root_page_num(0) {
        if (pager.num_pages == 0) {
            // New DB, Init Root
            void* root = pager.get_page(0);
            LeafNode node(root);
            node.initialize();
            node.set_root(true);
        }
    }

    void insert(uint32_t id, Row& row) {
        Cursor cursor = find(id);
        LeafNode leaf(pager.get_page(cursor.page_num));

        if (leaf.get_num_cells() >= LEAF_MAX_CELLS) {
            split_leaf(cursor, id, row);
        } else {
            leaf.insert(id, &row);
            std::cout << "Executed. (Inserted into Page " << cursor.page_num << ")\n";
        }
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
        _select_all(root_page_num);
    }

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
        
        uint32_t new_page_num = pager.get_unused_page_num();
        void* new_node_raw = pager.get_page(new_page_num);
        LeafNode new_node(new_node_raw);
        new_node.initialize();

        // Distribute Cells
        // Move top half to new node
        uint32_t total = old_node.get_num_cells();
        uint32_t split_point = total / 2;

        for (uint32_t i = split_point; i < total; i++) {
            Row* src = old_node.get_cell(i);
            new_node.insert(src->id, src);
        }
        old_node.set_num_cells(split_point);

        // Insert new key into correct half
        if (new_key < new_node.get_key(0)) {
            old_node.insert(new_key, &new_row);
        } else {
            new_node.insert(new_key, &new_row);
        }

        // Parent Update Logic
        if (old_node.is_root()) {
            // Need to move Old Node (Page 0) to a new Page ID so Page 0 can be Root
            uint32_t left_copy_page = pager.get_unused_page_num();
            void* left_copy = pager.get_page(left_copy_page);
            std::memcpy(left_copy, old_node_raw, PAGE_SIZE);
            
            // CRITICAL: Clear is_root on the copy (it's no longer the root)
            LeafNode left_leaf(left_copy);
            left_leaf.set_root(false);
            
            // Re-init old root as Internal
            InternalNode root(old_node_raw);
            root.initialize();
            root.set_root(true);
            // CRITICAL: Set num_keys BEFORE set_child, otherwise set_child(0,...)
            // sees num_keys==0 and writes to right_child instead of cell[0].
            root.set_num_keys(1);
            root.set_right_child(new_page_num);
            root.set_child(0, left_copy_page);
            root.set_key(0, new_node.get_key(0)); // Smallest key of right child becomes separator
            
            std::cout << "DEBUG: Root Split. New structure: Left(" << left_copy_page << ") Key(" << new_node.get_key(0) << ") Right(" << new_page_num << ")\n";
        } else {
            // Internal Node Update using Stack
            // The parent is the last element in the path stack
            uint32_t parent_page = cursor.path_stack.back();
            InternalNode parent(pager.get_page(parent_page)); 
            
            if (parent.get_num_keys() >= INTERNAL_MAX_CELLS) {
                std::cout << "ERROR: Max depth reached (Internal Split not fully implemented in this demo).\n";
            } else {
                // We need to find the INDEX where the split child was.
                // We know 'page_num' (the left child).
                // We search for it in parent.
                
                uint32_t child_index = 99999;
                uint32_t p_keys = parent.get_num_keys();
                // Check normal children
                for(uint32_t i=0; i<p_keys; i++) {
                    if (parent.get_cell(i) && *parent.get_cell(i) == page_num) {
                        child_index = i;
                        break;
                    } 
                }
                // Check right child
                if (child_index == 99999) {
                     if (parent.get_right_child() == page_num) {
                         child_index = p_keys;
                     }
                }
                
                if (child_index == 99999) {
                    std::cout << "CRITICAL ERROR: Could not find child in parent!\n";
                    return;
                }

                // Insert the new key logic
                parent.insert_child(child_index, new_node.get_key(0), new_page_num);
                std::cout << "DEBUG: Internal Update. Added child " << new_page_num << " at index " << child_index << "\n";
            }
        }
    }

    void _print_tree(uint32_t page_num, uint32_t level) {
        void* node_raw = pager.get_page(page_num);
        Node node(node_raw);
        
        for (uint32_t i = 0; i < level; i++) std::cout << "  ";

        if (node.get_type() == NODE_LEAF) {
            LeafNode leaf(node_raw);
            std::cout << "- LEAF (Page " << page_num << ") | " << leaf.get_num_cells() << " rows\n";
            for(uint32_t i=0; i<leaf.get_num_cells(); i++) {
                 for (uint32_t j = 0; j < level+1; j++) std::cout << "  ";
                 std::cout << leaf.get_key(i) << "\n";
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

    // In-order traversal of all leaf nodes
    void _select_all(uint32_t page_num) {
        void* node_raw = pager.get_page(page_num);
        Node node(node_raw);

        if (node.get_type() == NODE_LEAF) {
            LeafNode leaf(node_raw);
            for (uint32_t i = 0; i < leaf.get_num_cells(); i++) {
                Row* row = leaf.get_cell(i);
                std::cout << "  (" << row->id << ", " << row->username << ", " << row->email << ")\n";
            }
        } else {
            InternalNode internal(node_raw);
            for (uint32_t i = 0; i < internal.get_num_keys(); i++) {
                _select_all(internal.get_child(i));
            }
            _select_all(internal.get_right_child());
        }
    }
};

// ==========================================
// MAIN DRIVER
// ==========================================
int main() {
    std::cout << "ForgeDB v1.0 (OOP Edition)\n";
    Pager pager("my_database.db");
    BTree tree(pager);

    std::string input;
    while (true) {
        std::cout << "db > " << std::flush;
        std::getline(std::cin, input);

        if (input == "exit") break;
        if (input.substr(0, 6) == "insert") {
            Row row;
            char buf[100];
            std::sscanf(input.c_str(), "%99s %d %31s %254s", buf, &row.id, row.username, row.email);
            tree.insert(row.id, row);
        } else if (input == "select") {
            tree.select_all();
        } else if (input == ".tree") {
            tree.print_tree();
        } else if (input == ".json") {
            tree.print_json();
        } else {
            std::cout << "Unrecognized command.\n";
        }
    }
    return 0;
}