#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>

struct TermEntry {
    char* term;
    std::uint32_t* postings;
    std::uint32_t postings_count;
    std::uint32_t postings_cap;
    std::uint32_t last_doc_id;
    std::uint64_t postings_offset_bytes;
    int used;
};

struct DocMeta {
    std::uint32_t doc_id;
    char* title;
    char* url;
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

static std::uint32_t parse_u32(const char* s) {
    return static_cast<std::uint32_t>(std::strtoul(s, nullptr, 10));
}

static int ensure_postings_cap(TermEntry* entry, std::uint32_t need) {
    if (entry->postings_cap >= need) {
        return 1;
    }
    std::uint32_t new_cap = entry->postings_cap == 0 ? 8 : entry->postings_cap;
    while (new_cap < need) {
        if (new_cap > 0x7fffffffU) {
            return 0;
        }
        new_cap *= 2;
    }
    std::uint32_t* new_data = static_cast<std::uint32_t*>(
        std::realloc(entry->postings, static_cast<size_t>(new_cap) * sizeof(std::uint32_t)));
    if (!new_data) {
        return 0;
    }
    entry->postings = new_data;
    entry->postings_cap = new_cap;
    return 1;
}

static int add_term_doc(TermEntry* table, size_t capacity, const char* term, std::uint32_t doc_id,
                        std::uint64_t* used_terms) {
    std::uint64_t hash = djb2(term);
    size_t idx = static_cast<size_t>(hash % capacity);

    for (size_t step = 0; step < capacity; ++step) {
        TermEntry* entry = &table[idx];
        if (!entry->used) {
            entry->term = xstrdup(term);
            if (!entry->term) {
                return 0;
            }
            entry->postings = nullptr;
            entry->postings_count = 0;
            entry->postings_cap = 0;
            entry->last_doc_id = 0;
            entry->postings_offset_bytes = 0;
            entry->used = 1;
            ++(*used_terms);
        } else if (std::strcmp(entry->term, term) != 0) {
            idx = (idx + 1) % capacity;
            continue;
        }

        if (entry->postings_count == 0 || entry->last_doc_id != doc_id) {
            if (!ensure_postings_cap(entry, entry->postings_count + 1)) {
                return 0;
            }
            entry->postings[entry->postings_count] = doc_id;
            entry->postings_count += 1;
            entry->last_doc_id = doc_id;
        }
        return 1;
    }
    return 0;
}

static int cmp_term_ptrs(const void* a, const void* b) {
    const TermEntry* ta = *static_cast<TermEntry* const*>(a);
    const TermEntry* tb = *static_cast<TermEntry* const*>(b);
    return std::strcmp(ta->term, tb->term);
}

static int ensure_doc_meta_cap(DocMeta** metas, std::uint32_t* cap, std::uint32_t need) {
    if (*cap > need) {
        return 1;
    }
    std::uint32_t new_cap = (*cap == 0) ? 1024 : *cap;
    while (new_cap <= need) {
        if (new_cap > 0x7fffffffU) {
            return 0;
        }
        new_cap *= 2;
    }
    DocMeta* new_arr = static_cast<DocMeta*>(std::realloc(*metas, sizeof(DocMeta) * new_cap));
    if (!new_arr) {
        return 0;
    }
    for (std::uint32_t i = *cap; i < new_cap; ++i) {
        new_arr[i].doc_id = 0;
        new_arr[i].title = nullptr;
        new_arr[i].url = nullptr;
    }
    *metas = new_arr;
    *cap = new_cap;
    return 1;
}

static int write_u16(FILE* out, std::uint16_t v) {
    return std::fwrite(&v, sizeof(v), 1, out) == 1;
}

static int write_u32(FILE* out, std::uint32_t v) {
    return std::fwrite(&v, sizeof(v), 1, out) == 1;
}

static int write_u64(FILE* out, std::uint64_t v) {
    return std::fwrite(&v, sizeof(v), 1, out) == 1;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "Usage: index_builder <stemmed.txt> <raw_text.tsv> <index_dir> [hash_capacity]\n");
        return 1;
    }

    const char* stemmed_path = argv[1];
    const char* raw_text_path = argv[2];
    const char* out_dir = argv[3];
    size_t term_hash_capacity = (argc >= 5) ? static_cast<size_t>(std::strtoull(argv[4], nullptr, 10)) : (1u << 20);
    if (term_hash_capacity < 1024) {
        term_hash_capacity = 1024;
    }

    if (mkdir(out_dir, 0755) != 0 && errno != EEXIST) {
        std::fprintf(stderr, "Failed to create index dir %s: %s\n", out_dir, std::strerror(errno));
        return 1;
    }

    FILE* in_stemmed = std::fopen(stemmed_path, "rb");
    if (!in_stemmed) {
        std::fprintf(stderr, "Failed to open stemmed file: %s\n", stemmed_path);
        return 1;
    }

    TermEntry* term_table = static_cast<TermEntry*>(std::calloc(term_hash_capacity, sizeof(TermEntry)));
    if (!term_table) {
        std::fprintf(stderr, "Failed to allocate term table\n");
        std::fclose(in_stemmed);
        return 1;
    }

    char* line = nullptr;
    size_t line_cap = 0;
    std::uint64_t docs_indexed = 0;
    std::uint64_t tokens_seen = 0;
    std::uint64_t unique_terms = 0;

    while (1) {
        int n = read_line(in_stemmed, &line, &line_cap);
        if (n < 0) {
            break;
        }
        char* tab = std::strchr(line, '\t');
        if (!tab) {
            continue;
        }
        *tab = '\0';
        std::uint32_t doc_id = parse_u32(line);
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
                if (!add_term_doc(term_table, term_hash_capacity, start, doc_id, &unique_terms)) {
                    std::fprintf(stderr, "Failed to add term to index (table full or OOM)\n");
                    std::free(line);
                    std::fclose(in_stemmed);
                    for (size_t i = 0; i < term_hash_capacity; ++i) {
                        if (term_table[i].used) {
                            std::free(term_table[i].term);
                            std::free(term_table[i].postings);
                        }
                    }
                    std::free(term_table);
                    return 1;
                }
                ++tokens_seen;
            }
            if (!saved) {
                break;
            }
            *p = saved;
        }
        ++docs_indexed;
    }
    std::fclose(in_stemmed);

    TermEntry** sorted_terms = static_cast<TermEntry**>(std::malloc(sizeof(TermEntry*) * unique_terms));
    if (!sorted_terms) {
        std::fprintf(stderr, "Failed to allocate sorted term list\n");
        std::free(line);
        for (size_t i = 0; i < term_hash_capacity; ++i) {
            if (term_table[i].used) {
                std::free(term_table[i].term);
                std::free(term_table[i].postings);
            }
        }
        std::free(term_table);
        return 1;
    }

    std::uint64_t term_i = 0;
    for (size_t i = 0; i < term_hash_capacity; ++i) {
        if (term_table[i].used) {
            sorted_terms[term_i++] = &term_table[i];
        }
    }
    std::qsort(sorted_terms, static_cast<size_t>(unique_terms), sizeof(TermEntry*), cmp_term_ptrs);

    char postings_path[2048];
    char lexicon_path[2048];
    char forward_path[2048];
    std::snprintf(postings_path, sizeof(postings_path), "%s/postings.bin", out_dir);
    std::snprintf(lexicon_path, sizeof(lexicon_path), "%s/lexicon.bin", out_dir);
    std::snprintf(forward_path, sizeof(forward_path), "%s/forward.bin", out_dir);

    FILE* postings = std::fopen(postings_path, "wb");
    if (!postings) {
        std::fprintf(stderr, "Failed to open postings output\n");
        std::free(sorted_terms);
        std::free(line);
        for (size_t i = 0; i < term_hash_capacity; ++i) {
            if (term_table[i].used) {
                std::free(term_table[i].term);
                std::free(term_table[i].postings);
            }
        }
        std::free(term_table);
        return 1;
    }

    const std::uint32_t postings_magic = 0x504F5354U;
    const std::uint32_t version = 1;
    std::uint64_t total_postings = 0;
    write_u32(postings, postings_magic);
    write_u32(postings, version);
    write_u64(postings, total_postings);

    std::uint64_t offset = 0;
    for (std::uint64_t i = 0; i < unique_terms; ++i) {
        TermEntry* e = sorted_terms[i];
        e->postings_offset_bytes = offset;
        if (e->postings_count > 0) {
            std::fwrite(e->postings, sizeof(std::uint32_t), e->postings_count, postings);
            offset += static_cast<std::uint64_t>(e->postings_count) * sizeof(std::uint32_t);
            total_postings += e->postings_count;
        }
    }
    std::fseek(postings, static_cast<long>(sizeof(std::uint32_t) * 2), SEEK_SET);
    write_u64(postings, total_postings);
    std::fclose(postings);

    FILE* lexicon = std::fopen(lexicon_path, "wb");
    if (!lexicon) {
        std::fprintf(stderr, "Failed to open lexicon output\n");
        std::free(sorted_terms);
        std::free(line);
        for (size_t i = 0; i < term_hash_capacity; ++i) {
            if (term_table[i].used) {
                std::free(term_table[i].term);
                std::free(term_table[i].postings);
            }
        }
        std::free(term_table);
        return 1;
    }
    const std::uint32_t lexicon_magic = 0x4C455849U;
    write_u32(lexicon, lexicon_magic);
    write_u32(lexicon, version);
    write_u32(lexicon, static_cast<std::uint32_t>(unique_terms));
    for (std::uint64_t i = 0; i < unique_terms; ++i) {
        TermEntry* e = sorted_terms[i];
        size_t term_len = std::strlen(e->term);
        if (term_len > 65535) {
            term_len = 65535;
        }
        write_u16(lexicon, static_cast<std::uint16_t>(term_len));
        std::fwrite(e->term, 1, term_len, lexicon);
        write_u64(lexicon, e->postings_offset_bytes);
        write_u32(lexicon, e->postings_count);
    }
    std::fclose(lexicon);

    FILE* in_raw = std::fopen(raw_text_path, "rb");
    if (!in_raw) {
        std::fprintf(stderr, "Failed to open raw_text.tsv: %s\n", raw_text_path);
        std::free(sorted_terms);
        std::free(line);
        for (size_t i = 0; i < term_hash_capacity; ++i) {
            if (term_table[i].used) {
                std::free(term_table[i].term);
                std::free(term_table[i].postings);
            }
        }
        std::free(term_table);
        return 1;
    }

    DocMeta* metas = nullptr;
    std::uint32_t metas_cap = 0;
    std::uint32_t docs_with_meta = 0;
    std::uint32_t max_doc_id = 0;

    while (1) {
        int n = read_line(in_raw, &line, &line_cap);
        if (n < 0) {
            break;
        }
        char* p1 = std::strchr(line, '\t');
        if (!p1) {
            continue;
        }
        char* p2 = std::strchr(p1 + 1, '\t');
        if (!p2) {
            continue;
        }
        char* p3 = std::strchr(p2 + 1, '\t');
        if (!p3) {
            continue;
        }
        char* p4 = std::strchr(p3 + 1, '\t');
        if (!p4) {
            continue;
        }

        *p1 = '\0';
        *p2 = '\0';
        *p3 = '\0';
        *p4 = '\0';

        std::uint32_t doc_id = parse_u32(line);
        const char* url = p2 + 1;
        const char* title = p3 + 1;
        if (doc_id == 0) {
            continue;
        }
        if (!ensure_doc_meta_cap(&metas, &metas_cap, doc_id)) {
            std::fprintf(stderr, "Failed to allocate doc meta array\n");
            std::fclose(in_raw);
            std::free(sorted_terms);
            std::free(line);
            for (size_t i = 0; i < term_hash_capacity; ++i) {
                if (term_table[i].used) {
                    std::free(term_table[i].term);
                    std::free(term_table[i].postings);
                }
            }
            std::free(term_table);
            return 1;
        }
        if (metas[doc_id].doc_id == 0) {
            metas[doc_id].doc_id = doc_id;
            metas[doc_id].title = xstrdup(title);
            metas[doc_id].url = xstrdup(url);
            if (!metas[doc_id].title || !metas[doc_id].url) {
                std::fprintf(stderr, "Out of memory for doc meta strings\n");
                std::fclose(in_raw);
                std::free(sorted_terms);
                std::free(line);
                for (size_t i = 0; i < term_hash_capacity; ++i) {
                    if (term_table[i].used) {
                        std::free(term_table[i].term);
                        std::free(term_table[i].postings);
                    }
                }
                std::free(term_table);
                return 1;
            }
            ++docs_with_meta;
            if (doc_id > max_doc_id) {
                max_doc_id = doc_id;
            }
        }
    }
    std::fclose(in_raw);

    FILE* forward = std::fopen(forward_path, "wb");
    if (!forward) {
        std::fprintf(stderr, "Failed to open forward output\n");
        std::free(sorted_terms);
        std::free(line);
        for (size_t i = 0; i < term_hash_capacity; ++i) {
            if (term_table[i].used) {
                std::free(term_table[i].term);
                std::free(term_table[i].postings);
            }
        }
        std::free(term_table);
        if (metas) {
            for (std::uint32_t i = 0; i < metas_cap; ++i) {
                std::free(metas[i].title);
                std::free(metas[i].url);
            }
            std::free(metas);
        }
        return 1;
    }

    const std::uint32_t forward_magic = 0x46575244U;
    write_u32(forward, forward_magic);
    write_u32(forward, version);
    write_u32(forward, docs_with_meta);
    write_u32(forward, max_doc_id);
    for (std::uint32_t i = 1; i <= max_doc_id; ++i) {
        if (!metas || i >= metas_cap || metas[i].doc_id == 0) {
            continue;
        }
        std::uint16_t title_len = static_cast<std::uint16_t>(std::strlen(metas[i].title));
        std::uint16_t url_len = static_cast<std::uint16_t>(std::strlen(metas[i].url));
        write_u32(forward, metas[i].doc_id);
        write_u16(forward, title_len);
        write_u16(forward, url_len);
        std::fwrite(metas[i].title, 1, title_len, forward);
        std::fwrite(metas[i].url, 1, url_len, forward);
    }
    std::fclose(forward);

    std::printf("Index builder finished\n");
    std::printf("documents_indexed=%llu\n", static_cast<unsigned long long>(docs_indexed));
    std::printf("tokens_seen=%llu\n", static_cast<unsigned long long>(tokens_seen));
    std::printf("unique_terms=%llu\n", static_cast<unsigned long long>(unique_terms));
    std::printf("total_postings=%llu\n", static_cast<unsigned long long>(total_postings));
    std::printf("docs_with_meta=%u\n", docs_with_meta);

    std::free(sorted_terms);
    std::free(line);
    for (size_t i = 0; i < term_hash_capacity; ++i) {
        if (term_table[i].used) {
            std::free(term_table[i].term);
            std::free(term_table[i].postings);
        }
    }
    std::free(term_table);

    if (metas) {
        for (std::uint32_t i = 0; i < metas_cap; ++i) {
            std::free(metas[i].title);
            std::free(metas[i].url);
        }
        std::free(metas);
    }

    return 0;
}
