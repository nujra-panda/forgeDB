#include "btree.h"
#include "pager.h"
#include <iostream>
#include <string>
#include <cstdio>
#include <cstring>

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
