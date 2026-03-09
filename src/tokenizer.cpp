#include "tokenizer.h"
#include <cctype>
#include <unordered_map>

void Token::debug_print() const {
    std::cout << "<Type: " << type << ", Val: \"" << lexeme << "\">\n";
}

Tokenizer::Tokenizer(const std::string& query) : input(query), pos(0) {}

char Tokenizer::current_char() const {
    if (pos >= input.length()) return '\0';
    return input[pos];
}

void Tokenizer::advance() { pos++; }

void Tokenizer::skip_whitespace() {
    while (std::isspace(current_char())) advance();
}

Token Tokenizer::read_identifier_or_keyword() {
    std::string result;
    while (std::isalnum(current_char()) || current_char() == '_') {
        result += current_char();
        advance();
    }
    
    // Convert to uppercase for case-insensitive keyword matching
    std::string upper_res = result;
    for (auto &c : upper_res) c = std::toupper(c);

    if (upper_res == "SELECT") return {TOKEN_SELECT, result};
    if (upper_res == "INSERT") return {TOKEN_INSERT, result};
    if (upper_res == "DELETE") return {TOKEN_DELETE, result};
    if (upper_res == "VALUES") return {TOKEN_VALUES, result};
    if (upper_res == "FROM")   return {TOKEN_FROM, result};
    if (upper_res == "WHERE")  return {TOKEN_WHERE, result};
    if (upper_res == "INTO")   return {TOKEN_INTO, result};

    return {TOKEN_IDENTIFIER, result};
}

Token Tokenizer::read_number() {
    std::string result;
    while (std::isdigit(current_char())) {
        result += current_char();
        advance();
    }
    return {TOKEN_NUMBER, result};
}

Token Tokenizer::read_string() {
    std::string result;
    advance(); // Skip opening quote
    while (current_char() != '\0' && current_char() != '\'') {
        result += current_char();
        advance();
    }
    if (current_char() == '\'') advance(); // Skip closing quote
    return {TOKEN_STRING, result};
}

std::vector<Token> Tokenizer::tokenize() {
    std::vector<Token> tokens;
    skip_whitespace();
    
    while (current_char() != '\0') {
        char c = current_char();
        
        if (std::isalpha(c)) {
            tokens.push_back(read_identifier_or_keyword());
        } else if (std::isdigit(c)) {
            tokens.push_back(read_number());
        } else if (c == '\'') {
            tokens.push_back(read_string());
        } else {
            switch (c) {
                case '*': tokens.push_back({TOKEN_ASTERISK, "*"}); advance(); break;
                case ',': tokens.push_back({TOKEN_COMMA, ","}); advance(); break;
                case '(': tokens.push_back({TOKEN_LPAREN, "("}); advance(); break;
                case ')': tokens.push_back({TOKEN_RPAREN, ")"}); advance(); break;
                case '=': tokens.push_back({TOKEN_EQUALS, "="}); advance(); break;
                default:
                    tokens.push_back({TOKEN_ILLEGAL, std::string(1, c)});
                    advance();
                    break;
            }
        }
        skip_whitespace();
    }
    tokens.push_back({TOKEN_EOF, ""});
    return tokens;
}