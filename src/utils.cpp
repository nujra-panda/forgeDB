#include "utils.h"

// ==========================================
// CRC32 PAGE CHECKSUMS
// ==========================================
// Standard CRC32 (ISO 3309) with 0xEDB88320 polynomial.
// Computed over the full 4096-byte page with the checksum field zeroed.

static uint32_t crc32_table[256];
static bool     crc32_table_ready = false;

static void crc32_init() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_ready = true;
}

uint32_t crc32_compute(const uint8_t* buf, uint32_t len) {
    if (!crc32_table_ready) crc32_init();
    uint32_t crc = 0xFFFFFFFF;
    for (uint32_t i = 0; i < len; i++)
        crc = crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFF;
}

// ==========================================
// VARIABLE-LENGTH ROW SERIALIZATION
// ==========================================
// Wire format: [id:4B][username_len:2B][username:NB][email_len:2B][email:MB]
// Min size: 4+2+0+2+0 = 8 bytes   Max size: 4+2+31+2+254 = 293 bytes

uint16_t serialize_row(const Row& row, uint8_t* dest) {
    uint16_t off = 0;
    std::memcpy(dest + off, &row.id, 4);   off += 4;
    uint16_t ulen = (uint16_t)std::strlen(row.username);
    std::memcpy(dest + off, &ulen, 2);     off += 2;
    std::memcpy(dest + off, row.username, ulen);  off += ulen;
    uint16_t elen = (uint16_t)std::strlen(row.email);
    std::memcpy(dest + off, &elen, 2);     off += 2;
    std::memcpy(dest + off, row.email, elen);     off += elen;
    return off;
}

Row deserialize_row(const uint8_t* src) {
    Row row;
    std::memset(&row, 0, sizeof(Row));
    uint16_t off = 0;
    std::memcpy(&row.id, src + off, 4);   off += 4;
    uint16_t ulen;
    std::memcpy(&ulen, src + off, 2);     off += 2;
    std::memcpy(row.username, src + off, ulen);
    row.username[ulen] = '\0';            off += ulen;
    uint16_t elen;
    std::memcpy(&elen, src + off, 2);     off += 2;
    std::memcpy(row.email, src + off, elen);
    row.email[elen] = '\0';
    return row;
}

uint16_t serialized_row_size(const Row& row) {
    return 4 + 2 + (uint16_t)std::strlen(row.username) + 2 + (uint16_t)std::strlen(row.email);
}
