#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <vector>
#include <cstdint>

// --- CONSTANTS ---
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100; 

struct Row {
    uint32_t id;
    char username[32];
    char email[255];
};

const uint32_t ROW_SIZE = sizeof(Row);

// --- NODE HEADER CONSTANTS ---
const uint8_t NODE_INTERNAL = 0;
const uint8_t NODE_LEAF = 1;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t NODE_PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t NODE_NUM_CELLS_SIZE = sizeof(uint32_t);

const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + NODE_IS_ROOT_SIZE + NODE_PARENT_POINTER_SIZE + NODE_NUM_CELLS_SIZE;

// Leaf Node Layout
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// Internal Node Layout
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = sizeof(uint32_t) + sizeof(uint32_t); 

const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
// FIX 1: Define the max cells for internal nodes
const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

// Split Counts
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// --- FORWARD DECLARATION ---
struct Cursor; 

// --- NODE HELPER FUNCTIONS ---
uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)((char*)node + NODE_TYPE_SIZE + NODE_IS_ROOT_SIZE + NODE_PARENT_POINTER_SIZE);
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return (char*)node + LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE);
}

uint8_t get_node_type(void* node) {
    return *((uint8_t*)node);
}

void set_node_type(void* node, uint8_t type) {
    *((uint8_t*)node) = type;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    *leaf_node_num_cells(node) = 0;
}

// Internal Node Helpers
uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)((char*)node + COMMON_NODE_HEADER_SIZE);
}

uint32_t* internal_node_right_child(void* node) {
    return (uint32_t*)((char*)node + COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE);
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return (uint32_t*)((char*)node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    *internal_node_num_keys(node) = 0;
}

// --- PAGER STRUCT ---
struct Pager {
    std::fstream file_stream;
    uint32_t file_length;
    uint32_t num_pages; 
    void* pages[TABLE_MAX_PAGES]; 

    Pager(std::string filename) {
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) pages[i] = nullptr;
        
        std::ifstream check_file(filename);
        if (!check_file.good()) {
            std::ofstream create_file(filename);
            create_file.close();
        }
        check_file.close();
        
        file_stream.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        file_stream.seekp(0, std::ios::end);
        file_length = file_stream.tellp();
        num_pages = file_length / PAGE_SIZE; 
        
        if (file_length % PAGE_SIZE) num_pages++;
    }

    void* get_page(uint32_t page_num) {
        if (page_num >= TABLE_MAX_PAGES) {
            std::cout << "Error: Page number out of bounds.\n";
            return nullptr;
        }
        if (pages[page_num] != nullptr) return pages[page_num];
        
        void* page = calloc(1, PAGE_SIZE); 
        uint32_t pages_on_disk = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE) pages_on_disk++;

        if (page_num < pages_on_disk) {
            file_stream.seekg(page_num * PAGE_SIZE);
            file_stream.read((char*)page, PAGE_SIZE);
            file_stream.clear(); 
        } else {
            initialize_leaf_node(page);
            if (page_num >= num_pages) {
                num_pages = page_num + 1;
            }
        }
        pages[page_num] = page;
        return page;
    }

    void flush(uint32_t page_num) {
        if (pages[page_num] == nullptr) return;
        file_stream.seekp(page_num * PAGE_SIZE);
        file_stream.write((char*)pages[page_num], PAGE_SIZE);
        file_stream.flush();
    }
};

// --- TABLE STRUCT ---
struct Table {
    uint32_t root_page_num;
    Pager* pager;

    Table(std::string filename) {
        pager = new Pager(filename);
        root_page_num = 0; 
    }

    ~Table() {
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
            if (pager->pages[i]) {
                pager->flush(i);
                free(pager->pages[i]);
            }
        }
        delete pager;
    }

    Cursor* start();
    Cursor* end(); 
};

// --- CURSOR STRUCT ---
struct Cursor {
    Table* table;
    uint32_t row_num;
    bool end_of_table;

    Cursor(Table* t, uint32_t row) {
        table = t;
        row_num = row;
        end_of_table = false; 
    }
};

Cursor* Table::start() { return new Cursor(this, 0); }
Cursor* Table::end() { return new Cursor(this, 0); } 

// --- SPLIT LOGIC ---

void create_new_root(Table& table, uint32_t right_child_page_num) {
    void* root = table.pager->get_page(table.root_page_num);
    void* right_child = table.pager->get_page(right_child_page_num);
    
    uint32_t left_child_page_num = table.pager->num_pages;
    void* left_child = table.pager->get_page(left_child_page_num);

    std::memcpy(left_child, root, PAGE_SIZE);
    set_node_type(left_child, NODE_LEAF);

    initialize_internal_node(root);
    *internal_node_num_keys(root) = 1;
    
    // FIX 2: Cast void* to Row* before reading the ID
    Row* left_child_max_row = (Row*)leaf_node_cell(left_child, LEAF_NODE_LEFT_SPLIT_COUNT - 1);
    uint32_t left_child_max_key = left_child_max_row->id;

    *internal_node_cell(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *(uint32_t*)((char*)root + INTERNAL_NODE_HEADER_SIZE) = left_child_page_num;
    
    std::cout << "DEBUG: Split Complete. Root is now Internal. Left: " << left_child_page_num << ", Right: " << right_child_page_num << "\n";
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t id, char* username, char* email) {
    void* old_node = cursor->table->pager->get_page(cursor->row_num / LEAF_NODE_MAX_CELLS);
    uint32_t old_max = *leaf_node_num_cells(old_node);
    
    uint32_t new_page_num = cursor->table->pager->num_pages;
    void* new_node = cursor->table->pager->get_page(new_page_num);
    initialize_leaf_node(new_node);
    
    // All existing keys plus new key should be divided
    // effectively half go to old, half to new.
    // Since we are appending, we keep it simple:
    // Move the upper half of Old Node into New Node.
    
    *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    
    // --- FIX: COPY DATA LOOP ---
    // Move the top half of the rows from Old Node to New Node
    for (int32_t i = LEAF_NODE_LEFT_SPLIT_COUNT; i < old_max; i++) {
        void* source = leaf_node_cell(old_node, i);
        void* dest = leaf_node_cell(new_node, i - LEAF_NODE_LEFT_SPLIT_COUNT);
        std::memcpy(dest, source, ROW_SIZE);
    }
    // ---------------------------

    // Insert the NEW row. 
    // If it belongs in the new node (which it does, since we are appending), put it there.
    Row* row_location = (Row*)leaf_node_cell(new_node, *leaf_node_num_cells(new_node) - 1); 
    row_location->id = id;
    strncpy(row_location->username, username, 32);
    strncpy(row_location->email, email, 255);
    
    if (cursor->table->root_page_num == (cursor->row_num / LEAF_NODE_MAX_CELLS)) {
        create_new_root(*(cursor->table), new_page_num);
    } else {
        // Update Parent (Page 0)
        uint32_t parent_page_num = cursor->table->root_page_num;
        void* parent = cursor->table->pager->get_page(parent_page_num);
        
        uint32_t num_keys = *internal_node_num_keys(parent);
        
        // Take the max key from the OLD node (which is now on the left)
        Row* max_row = (Row*)leaf_node_cell(old_node, LEAF_NODE_LEFT_SPLIT_COUNT - 1);
        uint32_t key = max_row->id;
        
        uint32_t* right_child_ptr = internal_node_right_child(parent);
        uint32_t old_right_child = *right_child_ptr;
        
        uint32_t* cell = internal_node_cell(parent, num_keys);
        *cell = old_right_child; // Pointer
        *(cell + 1) = key;       // Key
        
        *right_child_ptr = new_page_num;
        *internal_node_num_keys(parent) += 1;
        
        std::cout << "DEBUG: Updated Parent (Page 0). Added Key: " << key << " pointing to Page " << old_right_child << ". New Right Child: " << new_page_num << "\n";
    }
}

// --- EXECUTE ---
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = table->pager->get_page(root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return new Cursor(table, root_page_num * LEAF_NODE_MAX_CELLS);
    }

    uint32_t* right_child_ptr = internal_node_right_child(root_node);
    uint32_t child_page_num = *right_child_ptr;
    
    return new Cursor(table, child_page_num * LEAF_NODE_MAX_CELLS);
}

void execute_insert(std::string input, Table& table) {
    Row row;
    char buffer[100];
    sscanf(input.c_str(), "%s %d %s %s", buffer, &row.id, row.username, row.email);

    Cursor* cursor = table_find(&table, row.id);
    
    uint32_t page_num = cursor->row_num / LEAF_NODE_MAX_CELLS;
    void* page = table.pager->get_page(page_num);
    uint32_t num_cells = *leaf_node_num_cells(page);
    
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, row.id, row.username, row.email);
        delete cursor;
        return;
    }

    Row* row_location = (Row*)leaf_node_cell(page, num_cells);
    row_location->id = row.id;
    strncpy(row_location->username, row.username, 32);
    strncpy(row_location->email, row.email, 255);

    *leaf_node_num_cells(page) += 1;
    std::cout << "Executed. (Inserted into Page " << page_num << ", Cell " << num_cells << ")\n";
    
    delete cursor;
}

void execute_select(Table& table) {
    uint32_t num_pages = table.pager->num_pages;
    if (num_pages == 0) {
        num_pages = table.pager->file_length / PAGE_SIZE;
        if (table.pager->file_length % PAGE_SIZE) num_pages++;
    }

    for (uint32_t i = 0; i < num_pages; i++) {
        void* page = table.pager->get_page(i);
        if (get_node_type(page) == NODE_INTERNAL) {
            std::cout << "Page " << i << ": (Internal Node - Skipping)\n";
            continue;
        }

        uint32_t num_cells = *leaf_node_num_cells(page);
        std::cout << "Page " << i << " (" << num_cells << " rows):\n";
        for (uint32_t c = 0; c < num_cells; c++) {
            Row* row = (Row*)leaf_node_cell(page, c);
            std::cout << "  - " << row->id << ": " << row->username << " (" << row->email << ")\n";
        }
    }
}

void print_indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) std::cout << "  ";
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t level) {
    void* node = pager->get_page(page_num);
    uint32_t num_keys, child;

    print_indent(level);
    
    if (get_node_type(node) == NODE_LEAF) {
        uint32_t num_cells = *leaf_node_num_cells(node);
        std::cout << "- LEAF (Page " << page_num << ") | Cells: " << num_cells << "\n";
        for (uint32_t i = 0; i < num_cells; i++) {
            print_indent(level + 1);
            Row* row = (Row*)leaf_node_cell(node, i);
            std::cout << row->id << "\n";
        }
    } else if (get_node_type(node) == NODE_INTERNAL) {
        uint32_t num_keys = *internal_node_num_keys(node);
        std::cout << "- INTERNAL (Page " << page_num << ") | Keys: " << num_keys << "\n";
        for (uint32_t i = 0; i < num_keys; i++) {
            child = *internal_node_cell(node, i);
            print_tree(pager, child, level + 1);
            print_indent(level + 1);
            // Fix: Cast memory to uint32_t* before dereferencing key
            uint32_t* key_ptr = internal_node_cell(node, i) + 1;
            std::cout << "Key: " << *key_ptr << "\n";
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, level + 1);
    }
}

int main() {
    Table table("/app/data/my_database.db");
    std::cout << "ForgeDB v0.7 - Internal Node Update Edition\n";
    std::cout << "Max cells per page: " << LEAF_NODE_MAX_CELLS << "\n";
    
    while (true) {
        std::cout << "db > ";
        std::string input_buffer;
        std::getline(std::cin, input_buffer);

        if (input_buffer == "exit") break;
        if (input_buffer.substr(0, 6) == "insert") execute_insert(input_buffer, table);
        else if (input_buffer.substr(0, 6) == "select") execute_select(table);
        else if (input_buffer == ".tree") {
            print_tree(table.pager, table.root_page_num, 0);
            continue;
        }   
        else std::cout << "Unrecognized command.\n";
    }
    return 0;
}