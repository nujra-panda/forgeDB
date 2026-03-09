#pragma once
#include <string>
#include <vector>
#include <iostream>

// The types of words your DB understands
enum TokenType {
    // Keywords
    TOKEN_SELECT, TOKEN_INSERT, TOKEN_DELETE, TOKEN_VALUES,
    TOKEN_FROM, TOKEN_WHERE, TOKEN_INTO,
    
    // Symbols
    TOKEN_ASTERISK, // *
    TOKEN_COMMA,    // ,
    TOKEN_LPAREN,   // (
    TOKEN_RPAREN,   // )
    TOKEN_EQUALS,   // =
    
    // Literals
    TOKEN_IDENTIFIER, // users, id, name (table/col names)
    TOKEN_NUMBER,     // 123
    TOKEN_STRING,     // 'alice'
    
    // Control
    TOKEN_EOF,
    TOKEN_ILLEGAL
};

struct Token {
    TokenType type;
    std::string lexeme;
    
    void debug_print() const;
};

class Tokenizer {
    std::string input;
    size_t pos;

    char current_char() const;
    void advance();
    void skip_whitespace();
    
    Token read_identifier_or_keyword();
    Token read_number();
    Token read_string();

public:
    Tokenizer(const std::string& query);
    std::vector<Token> tokenize();
};