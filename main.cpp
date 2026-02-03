#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <vector>
#include <cstdint>

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100; 

struct Row {
    uint32_t id;
    char username[32];
    char email[255];
};

const uint32_t ROW_SIZE = sizeof(Row);
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// --- FORWARD DECLARATION (The Fix) ---
struct Cursor; 

// --- PAGER STRUCT ---
struct Pager {
    std::fstream file_stream;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES]; 

    Pager(std::string filename) {
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
            pages[i] = nullptr;
        }
        std::ifstream check_file(filename);
        if (!check_file.good()) {
            std::ofstream create_file(filename);
            create_file.close();
        }
        check_file.close();
        file_stream.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        file_stream.seekp(0, std::ios::end);
        file_length = file_stream.tellp();
    }

    void* get_page(uint32_t page_num) {
        if (page_num >= TABLE_MAX_PAGES) {
            std::cout << "Error: Page number out of bounds.\n";
            return nullptr;
        }
        if (pages[page_num] != nullptr) {
            return pages[page_num];
        }
        
        void* page = calloc(1, PAGE_SIZE); 
        uint32_t num_pages = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE) num_pages += 1;

        if (page_num < num_pages) {
            file_stream.seekg(page_num * PAGE_SIZE);
            file_stream.read((char*)page, PAGE_SIZE);
            if (file_stream.fail() && !file_stream.eof()) {
                 std::cerr << "Error: Failed to read page from disk.\n";
            }
            file_stream.clear(); 
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
    uint32_t num_rows;
    Pager* pager;

    Table(std::string filename) {
        pager = new Pager(filename);
        num_rows = pager->file_length / ROW_SIZE;
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

    void* row_slot(uint32_t row_num) {
        uint32_t page_num = row_num / ROWS_PER_PAGE;
        void* page = pager->get_page(page_num);
        uint32_t row_offset = row_num % ROWS_PER_PAGE;
        uint32_t byte_offset = row_offset * ROW_SIZE;
        return (char*)page + byte_offset;
    }

    // DECLARE these now, DEFINE them later (Fixes the circular dependency)
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
        end_of_table = (row_num >= table->num_rows);
    }
};

// --- IMPLEMENT TABLE METHODS NOW ---
Cursor* Table::start() {
    return new Cursor(this, 0);
}

Cursor* Table::end() {
    return new Cursor(this, num_rows);
}

// --- CURSOR HELPERS ---
void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->row_num / ROWS_PER_PAGE;
    void* page = cursor->table->pager->get_page(page_num);
    uint32_t row_offset = cursor->row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (char*)page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}

// --- EXECUTE FUNCTIONS ---
void execute_insert(std::string input, Table& table) {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        std::cout << "Error: Table full.\n";
        return;
    }
    Cursor* cursor = table.end();
    Row* row_location = (Row*)cursor_value(cursor);
    char command_buffer[100];
    sscanf(input.c_str(), "%s %d %s %s", command_buffer, &(row_location->id), row_location->username, row_location->email);
    table.num_rows += 1;
    std::cout << "Executed.\n";
    delete cursor;
}

void execute_select(Table& table) {
    Cursor* cursor = table.start();
    while (!cursor->end_of_table) {
        Row* row = (Row*)cursor_value(cursor);
        if (row->id != 0) {
            std::cout << "(" << row->id << ", " << row->username << ", " << row->email << ")\n";
        }
        cursor_advance(cursor);
    }
    delete cursor;
}

// --- MAIN ---
int main() {
    Table table("/app/data/my_database.db");
    std::cout << "ForgeDB v0.4 - The Cursor Edition\n";
    
    while (true) {
        std::cout << "db > ";
        std::string input_buffer;
        std::getline(std::cin, input_buffer);

        if (input_buffer == "exit") break;
        if (input_buffer.substr(0, 6) == "insert") execute_insert(input_buffer, table);
        else if (input_buffer.substr(0, 6) == "select") execute_select(table);
        else std::cout << "Unrecognized command.\n";
    }
    return 0;
}