#pragma once
#include "tokenizer.h"
#include "common.h" // For struct Row
#include <vector>
#include <string>
#include <iostream>

// Types of SQL statements we support
enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_DELETE
};

// The Execution Plan
struct Statement {
    StatementType type;
    Row row_to_insert;     // Payload for INSERT
    uint32_t target_id;    // Payload for DELETE/SELECT
};

class Parser {
    std::vector<Token> tokens;
    size_t pos;

    Token current_token() const;
    void advance();
    bool match(TokenType expected); // Checks type and advances if matches

    bool parse_insert(Statement& statement);

public:
    Parser(const std::vector<Token>& tokens);
    bool parse_statement(Statement& statement);
};