#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

struct TermSlot {
    char* term;
    std::uint32_t count;
    int used;
};

struct TermRow {
    char* term;
    std::uint32_t count;
};

static int read_line(FILE* in, char** buffer, size_t* capacity) {
    if (*buffer == nullptr || *capacity == 0) {
        *capacity = 4096;
        *buffer = static_cast<char*>(std::malloc(*capacity));
        if (!*buffer) {
            return -1;
        }
    }
    size_t len = 0;
    while (1) {
        int c = std::fgetc(in);
        if (c == EOF) {
            if (len == 0) {
                return -1;
            }
            break;
        }
        if (len + 1 >= *capacity) {
            size_t new_cap = (*capacity) * 2;
            char* new_buf = static_cast<char*>(std::realloc(*buffer, new_cap));
            if (!new_buf) {
                return -1;
            }
            *buffer = new_buf;
            *capacity = new_cap;
        }
        (*buffer)[len++] = static_cast<char>(c);
        if (c == '\n') {
            break;
        }
    }
    (*buffer)[len] = '\0';
    return static_cast<int>(len);
}

static std::uint64_t djb2(const char* s) {
    std::uint64_t hash = 5381;
    while (*s) {
        hash = ((hash << 5) + hash) + static_cast<unsigned char>(*s);
        ++s;
    }
    return hash;
}

static char* xstrdup(const char* s) {
    size_t n = std::strlen(s);
    char* out = static_cast<char*>(std::malloc(n + 1));
    if (!out) {
        return nullptr;
    }
    std::memcpy(out, s, n + 1);
    return out;
}

static int term_compare(const void* a, const void* b) {
    const TermRow* ta = static_cast<const TermRow*>(a);
    const TermRow* tb = static_cast<const TermRow*>(b);
    if (ta->count > tb->count) {
        return -1;
    }
    if (ta->count < tb->count) {
        return 1;
    }
    return std::strcmp(ta->term, tb->term);
}

static int add_term(TermSlot* table, std::size_t capacity, const char* term, std::uint64_t* used_slots) {
    std::uint64_t hash = djb2(term);
    std::size_t idx = static_cast<std::size_t>(hash % capacity);
    for (std::size_t step = 0; step < capacity; ++step) {
        TermSlot* slot = &table[idx];
        if (!slot->used) {
            slot->term = xstrdup(term);
            if (!slot->term) {
                return 0;
            }
            slot->count = 1;
            slot->used = 1;
            ++(*used_slots);
            return 1;
        }
        if (std::strcmp(slot->term, term) == 0) {
            slot->count += 1;
            return 1;
        }
        idx = (idx + 1) % capacity;
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: term_stats <stemmed.txt> <term_freq.csv> [hash_capacity]\n");
        return 1;
    }

    std::size_t capacity = (argc >= 4) ? static_cast<std::size_t>(std::strtoull(argv[3], nullptr, 10)) : (1u << 20);
    if (capacity < 1024) {
        capacity = 1024;
    }

    FILE* in = std::fopen(argv[1], "rb");
    if (!in) {
        std::fprintf(stderr, "Failed to open input: %s\n", argv[1]);
        return 1;
    }
    FILE* out = std::fopen(argv[2], "wb");
    if (!out) {
        std::fprintf(stderr, "Failed to open output: %s\n", argv[2]);
        std::fclose(in);
        return 1;
    }

    TermSlot* table = static_cast<TermSlot*>(std::calloc(capacity, sizeof(TermSlot)));
    if (!table) {
        std::fprintf(stderr, "Failed to allocate hash table\n");
        std::fclose(in);
        std::fclose(out);
        return 1;
    }

    char* line = nullptr;
    size_t line_cap = 0;
    std::uint64_t docs = 0;
    std::uint64_t all_tokens = 0;
    std::uint64_t unique_terms = 0;
    std::uint64_t total_term_len = 0;

    while (1) {
        int line_len = read_line(in, &line, &line_cap);
        if (line_len < 0) {
            break;
        }
        if (line_len == 0) {
            continue;
        }
        char* tab = std::strchr(line, '\t');
        if (!tab) {
            continue;
        }
        char* body = tab + 1;

        char* p = body;
        while (*p) {
            while (*p && std::isspace(static_cast<unsigned char>(*p))) {
                ++p;
            }
            if (!*p) {
                break;
            }
            char* start = p;
            while (*p && !std::isspace(static_cast<unsigned char>(*p))) {
                ++p;
            }
            char saved = *p;
            *p = '\0';

            if (start[0] != '\0') {
                if (!add_term(table, capacity, start, &unique_terms)) {
                    std::fprintf(stderr, "Term hash table is full; increase capacity\n");
                    std::free(line);
                    std::fclose(in);
                    std::fclose(out);
                    for (std::size_t i = 0; i < capacity; ++i) {
                        if (table[i].used) {
                            std::free(table[i].term);
                        }
                    }
                    std::free(table);
                    return 1;
                }
                ++all_tokens;
                total_term_len += static_cast<std::uint64_t>(std::strlen(start));
            }

            if (!saved) {
                break;
            }
            *p = saved;
        }
        ++docs;
    }

    TermRow* rows = static_cast<TermRow*>(std::malloc(sizeof(TermRow) * unique_terms));
    if (!rows) {
        std::fprintf(stderr, "Failed to allocate rows\n");
        std::free(line);
        std::fclose(in);
        std::fclose(out);
        for (std::size_t i = 0; i < capacity; ++i) {
            if (table[i].used) {
                std::free(table[i].term);
            }
        }
        std::free(table);
        return 1;
    }

    std::uint64_t row_idx = 0;
    for (std::size_t i = 0; i < capacity; ++i) {
        if (!table[i].used) {
            continue;
        }
        rows[row_idx].term = table[i].term;
        rows[row_idx].count = table[i].count;
        ++row_idx;
    }

    std::qsort(rows, static_cast<size_t>(unique_terms), sizeof(TermRow), term_compare);

    std::fprintf(out, "term,count\n");
    for (std::uint64_t i = 0; i < unique_terms; ++i) {
        std::fprintf(out, "%s,%u\n", rows[i].term, rows[i].count);
    }

    double avg_term_len = all_tokens == 0 ? 0.0 : static_cast<double>(total_term_len) / static_cast<double>(all_tokens);
    std::printf("Term stats finished\n");
    std::printf("documents=%llu\n", static_cast<unsigned long long>(docs));
    std::printf("all_tokens=%llu\n", static_cast<unsigned long long>(all_tokens));
    std::printf("unique_terms=%llu\n", static_cast<unsigned long long>(unique_terms));
    std::printf("avg_term_length=%.4f\n", avg_term_len);

    std::free(rows);
    std::free(line);
    std::fclose(in);
    std::fclose(out);

    for (std::size_t i = 0; i < capacity; ++i) {
        if (table[i].used) {
            std::free(table[i].term);
        }
    }
    std::free(table);
    return 0;
}
