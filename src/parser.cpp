#include "parser.h"
#include <cstring>

Parser::Parser(const std::vector<Token>& tokens) : tokens(tokens), pos(0) {}

Token Parser::current_token() const {
    if (pos >= tokens.size()) return {TOKEN_EOF, ""};
    return tokens[pos];
}

void Parser::advance() {
    if (pos < tokens.size()) pos++;
}

bool Parser::match(TokenType expected) {
    if (current_token().type == expected) {
        advance();
        return true;
    }
    return false;
}

bool Parser::parse_insert(Statement& statement) {
    statement.type = STATEMENT_INSERT;
    std::memset(&statement.row_to_insert, 0, sizeof(Row));

    // We already matched 'INSERT', so next should be 'INTO'
    if (!match(TOKEN_INTO)) return false;
    
    // For now, we ignore the table name since we only have one table
    if (!match(TOKEN_IDENTIFIER)) return false; 
    
    if (!match(TOKEN_VALUES)) return false;
    if (!match(TOKEN_LPAREN)) return false;

    // Parse ID (Number)
    if (current_token().type != TOKEN_NUMBER) return false;
    statement.row_to_insert.id = std::stoul(current_token().lexeme);
    advance();

    if (!match(TOKEN_COMMA)) return false;

    // Parse Username (String)
    if (current_token().type != TOKEN_STRING) return false;
    std::strncpy(statement.row_to_insert.username, current_token().lexeme.c_str(), 31);
    advance();

    if (!match(TOKEN_COMMA)) return false;

    // Parse Email (String)
    if (current_token().type != TOKEN_STRING) return false;
    std::strncpy(statement.row_to_insert.email, current_token().lexeme.c_str(), 254);
    advance();

    if (!match(TOKEN_RPAREN)) return false;

    return true; // Successfully parsed an INSERT statement!
}

bool Parser::parse_statement(Statement& statement) {
    if (match(TOKEN_INSERT)) {
        return parse_insert(statement);
    }
    // (We will add SELECT and DELETE here later)
    
    std::cout << "Syntax Error: Unrecognized statement.\n";
    return false;
}