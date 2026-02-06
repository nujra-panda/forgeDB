#pragma once
#include "common.h"

// ==========================================
// CRC32 PAGE CHECKSUMS (ISO 3309, 0xEDB88320)
// ==========================================
uint32_t crc32_compute(const uint8_t* buf, uint32_t len);

// ==========================================
// VARIABLE-LENGTH ROW SERIALIZATION
// ==========================================
// Wire format: [id:4B][username_len:2B][username:NB][email_len:2B][email:MB]
// Min size: 4+2+0+2+0 = 8 bytes   Max size: 4+2+31+2+254 = 293 bytes
uint16_t serialize_row(const Row& row, uint8_t* dest);
Row deserialize_row(const uint8_t* src);
uint16_t serialized_row_size(const Row& row);
