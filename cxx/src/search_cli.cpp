#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

enum TokenType {
    TOK_TERM = 1,
    TOK_AND = 2,
    TOK_OR = 3,
    TOK_NOT = 4,
    TOK_LPAREN = 5,
    TOK_RPAREN = 6
};

struct LexEntry {
    char* term;
    std::uint64_t postings_offset;
    std::uint32_t postings_count;
};

struct DocMeta {
    char* title;
    char* url;
};

struct IndexData {
    LexEntry* lexicon;
    std::uint32_t term_count;

    std::uint32_t* postings_data;
    std::uint64_t postings_total;

    DocMeta* metas_by_id;
    std::uint32_t max_doc_id;
    std::uint32_t docs_with_meta;

    std::uint32_t* universe_ids;
    std::uint32_t universe_count;
};

struct Token {
    int type;
    char* text;
};

struct PostingList {
    std::uint32_t* ids;
    std::uint32_t count;
};

static int read_u16(FILE* in, std::uint16_t* out) {
    return std::fread(out, sizeof(*out), 1, in) == 1;
}
static int read_u32(FILE* in, std::uint32_t* out) {
    return std::fread(out, sizeof(*out), 1, in) == 1;
}
static int read_u64(FILE* in, std::uint64_t* out) {
    return std::fread(out, sizeof(*out), 1, in) == 1;
}

static char* xstrndup(const char* s, size_t n) {
    char* out = static_cast<char*>(std::malloc(n + 1));
    if (!out) {
        return nullptr;
    }
    if (n > 0) {
        std::memcpy(out, s, n);
    }
    out[n] = '\0';
    return out;
}

static int ends_with(const char* s, int n, const char* suffix) {
    int m = static_cast<int>(std::strlen(suffix));
    if (n < m) {
        return 0;
    }
    return std::memcmp(s + n - m, suffix, static_cast<size_t>(m)) == 0;
}

static void stem_term_inplace(char* token) {
    int n = static_cast<int>(std::strlen(token));
    if (n <= 2) {
        return;
    }
    if (n > 5 && ends_with(token, n, "ingly")) {
        token[n - 5] = '\0';
        return;
    }
    if (n > 4 && ends_with(token, n, "edly")) {
        token[n - 4] = '\0';
        return;
    }
    if (n > 4 && ends_with(token, n, "ing")) {
        token[n - 3] = '\0';
        return;
    }
    if (n > 3 && ends_with(token, n, "ed")) {
        token[n - 2] = '\0';
        return;
    }
    if (n > 4 && ends_with(token, n, "ies")) {
        token[n - 3] = 'y';
        token[n - 2] = '\0';
        return;
    }
    if (n > 3 && ends_with(token, n, "es")) {
        token[n - 2] = '\0';
        return;
    }
    if (n > 3 && ends_with(token, n, "ly")) {
        token[n - 2] = '\0';
        return;
    }
    if (n > 3 && token[n - 1] == 's') {
        token[n - 1] = '\0';
    }
}

static char* path_join3(const char* dir, const char* name) {
    size_t a = std::strlen(dir);
    size_t b = std::strlen(name);
    size_t need = a + 1 + b + 1;
    char* out = static_cast<char*>(std::malloc(need));
    if (!out) {
        return nullptr;
    }
    std::snprintf(out, need, "%s/%s", dir, name);
    return out;
}

static int load_postings(IndexData* idx, const char* postings_path) {
    FILE* in = std::fopen(postings_path, "rb");
    if (!in) {
        std::fprintf(stderr, "Failed to open %s\n", postings_path);
        return 0;
    }
    std::uint32_t magic = 0;
    std::uint32_t version = 0;
    std::uint64_t total = 0;
    if (!read_u32(in, &magic) || !read_u32(in, &version) || !read_u64(in, &total)) {
        std::fclose(in);
        return 0;
    }
    if (magic != 0x504F5354U || version != 1) {
        std::fclose(in);
        std::fprintf(stderr, "Invalid postings header\n");
        return 0;
    }
    idx->postings_data = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * static_cast<size_t>(total)));
    if (!idx->postings_data && total > 0) {
        std::fclose(in);
        return 0;
    }
    if (total > 0 && std::fread(idx->postings_data, sizeof(std::uint32_t), static_cast<size_t>(total), in) != total) {
        std::fclose(in);
        return 0;
    }
    std::fclose(in);
    idx->postings_total = total;
    return 1;
}

static int load_lexicon(IndexData* idx, const char* lexicon_path) {
    FILE* in = std::fopen(lexicon_path, "rb");
    if (!in) {
        std::fprintf(stderr, "Failed to open %s\n", lexicon_path);
        return 0;
    }
    std::uint32_t magic = 0;
    std::uint32_t version = 0;
    std::uint32_t term_count = 0;
    if (!read_u32(in, &magic) || !read_u32(in, &version) || !read_u32(in, &term_count)) {
        std::fclose(in);
        return 0;
    }
    if (magic != 0x4C455849U || version != 1) {
        std::fclose(in);
        std::fprintf(stderr, "Invalid lexicon header\n");
        return 0;
    }
    idx->lexicon = static_cast<LexEntry*>(std::calloc(term_count, sizeof(LexEntry)));
    if (!idx->lexicon && term_count > 0) {
        std::fclose(in);
        return 0;
    }

    for (std::uint32_t i = 0; i < term_count; ++i) {
        std::uint16_t term_len = 0;
        if (!read_u16(in, &term_len)) {
            std::fclose(in);
            return 0;
        }
        idx->lexicon[i].term = static_cast<char*>(std::malloc(static_cast<size_t>(term_len) + 1));
        if (!idx->lexicon[i].term) {
            std::fclose(in);
            return 0;
        }
        if (term_len > 0) {
            if (std::fread(idx->lexicon[i].term, 1, term_len, in) != term_len) {
                std::fclose(in);
                return 0;
            }
        }
        idx->lexicon[i].term[term_len] = '\0';
        if (!read_u64(in, &idx->lexicon[i].postings_offset) || !read_u32(in, &idx->lexicon[i].postings_count)) {
            std::fclose(in);
            return 0;
        }
    }
    std::fclose(in);
    idx->term_count = term_count;
    return 1;
}

static int ensure_meta_cap(DocMeta** metas, std::uint32_t* cap, std::uint32_t need) {
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
    DocMeta* arr = static_cast<DocMeta*>(std::realloc(*metas, sizeof(DocMeta) * new_cap));
    if (!arr) {
        return 0;
    }
    for (std::uint32_t i = *cap; i < new_cap; ++i) {
        arr[i].title = nullptr;
        arr[i].url = nullptr;
    }
    *metas = arr;
    *cap = new_cap;
    return 1;
}

static int load_forward(IndexData* idx, const char* forward_path) {
    FILE* in = std::fopen(forward_path, "rb");
    if (!in) {
        std::fprintf(stderr, "Failed to open %s\n", forward_path);
        return 0;
    }
    std::uint32_t magic = 0;
    std::uint32_t version = 0;
    std::uint32_t docs = 0;
    std::uint32_t max_doc_id = 0;
    if (!read_u32(in, &magic) || !read_u32(in, &version) || !read_u32(in, &docs) || !read_u32(in, &max_doc_id)) {
        std::fclose(in);
        return 0;
    }
    if (magic != 0x46575244U || version != 1) {
        std::fclose(in);
        std::fprintf(stderr, "Invalid forward header\n");
        return 0;
    }
    idx->docs_with_meta = docs;
    idx->max_doc_id = max_doc_id;

    DocMeta* metas = nullptr;
    std::uint32_t metas_cap = 0;
    if (!ensure_meta_cap(&metas, &metas_cap, max_doc_id)) {
        std::fclose(in);
        return 0;
    }

    std::uint32_t* universe = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * docs));
    if (!universe && docs > 0) {
        std::fclose(in);
        return 0;
    }
    std::uint32_t ucount = 0;

    for (std::uint32_t i = 0; i < docs; ++i) {
        std::uint32_t doc_id = 0;
        std::uint16_t title_len = 0;
        std::uint16_t url_len = 0;
        if (!read_u32(in, &doc_id) || !read_u16(in, &title_len) || !read_u16(in, &url_len)) {
            std::fclose(in);
            return 0;
        }
        char* title = static_cast<char*>(std::malloc(static_cast<size_t>(title_len) + 1));
        char* url = static_cast<char*>(std::malloc(static_cast<size_t>(url_len) + 1));
        if ((!title && title_len > 0) || (!url && url_len > 0)) {
            std::fclose(in);
            return 0;
        }
        if (title_len > 0 && std::fread(title, 1, title_len, in) != title_len) {
            std::fclose(in);
            return 0;
        }
        if (url_len > 0 && std::fread(url, 1, url_len, in) != url_len) {
            std::fclose(in);
            return 0;
        }
        title[title_len] = '\0';
        url[url_len] = '\0';

        if (!ensure_meta_cap(&metas, &metas_cap, doc_id)) {
            std::fclose(in);
            return 0;
        }
        metas[doc_id].title = title;
        metas[doc_id].url = url;
        universe[ucount++] = doc_id;
    }
    std::fclose(in);

    idx->metas_by_id = metas;
    idx->universe_ids = universe;
    idx->universe_count = ucount;
    return 1;
}

static void free_index(IndexData* idx) {
    if (!idx) {
        return;
    }
    if (idx->lexicon) {
        for (std::uint32_t i = 0; i < idx->term_count; ++i) {
            std::free(idx->lexicon[i].term);
        }
        std::free(idx->lexicon);
    }
    std::free(idx->postings_data);
    if (idx->metas_by_id) {
        for (std::uint32_t i = 0; i <= idx->max_doc_id; ++i) {
            std::free(idx->metas_by_id[i].title);
            std::free(idx->metas_by_id[i].url);
        }
        std::free(idx->metas_by_id);
    }
    std::free(idx->universe_ids);
}

static int lexicon_find(const IndexData* idx, const char* term, std::uint64_t* offset, std::uint32_t* count) {
    std::int64_t lo = 0;
    std::int64_t hi = static_cast<std::int64_t>(idx->term_count) - 1;
    while (lo <= hi) {
        std::int64_t mid = (lo + hi) / 2;
        int cmp = std::strcmp(term, idx->lexicon[mid].term);
        if (cmp == 0) {
            *offset = idx->lexicon[mid].postings_offset;
            *count = idx->lexicon[mid].postings_count;
            return 1;
        }
        if (cmp < 0) {
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }
    return 0;
}

static int token_push(Token** arr, std::uint32_t* count, std::uint32_t* cap, Token t) {
    if (*count >= *cap) {
        std::uint32_t new_cap = (*cap == 0) ? 16 : (*cap * 2);
        Token* new_arr = static_cast<Token*>(std::realloc(*arr, sizeof(Token) * new_cap));
        if (!new_arr) {
            return 0;
        }
        *arr = new_arr;
        *cap = new_cap;
    }
    (*arr)[*count] = t;
    (*count)++;
    return 1;
}

static int is_operand_end(int type) {
    return type == TOK_TERM || type == TOK_RPAREN;
}

static int is_operand_start(int type) {
    return type == TOK_TERM || type == TOK_LPAREN || type == TOK_NOT;
}

static int tokenize_query(const char* query, Token** out_tokens, std::uint32_t* out_count) {
    Token* raw = nullptr;
    std::uint32_t raw_count = 0;
    std::uint32_t raw_cap = 0;

    size_t n = std::strlen(query);
    size_t i = 0;
    while (i < n) {
        unsigned char ch = static_cast<unsigned char>(query[i]);
        if (std::isspace(ch)) {
            ++i;
            continue;
        }
        if (ch == '&' && i + 1 < n && query[i + 1] == '&') {
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_AND, nullptr})) {
                return 0;
            }
            i += 2;
            continue;
        }
        if (ch == '|' && i + 1 < n && query[i + 1] == '|') {
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_OR, nullptr})) {
                return 0;
            }
            i += 2;
            continue;
        }
        if (ch == '!') {
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_NOT, nullptr})) {
                return 0;
            }
            ++i;
            continue;
        }
        if (ch == '(') {
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_LPAREN, nullptr})) {
                return 0;
            }
            ++i;
            continue;
        }
        if (ch == ')') {
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_RPAREN, nullptr})) {
                return 0;
            }
            ++i;
            continue;
        }
        if (std::isalnum(ch)) {
            size_t start = i;
            while (i < n && std::isalnum(static_cast<unsigned char>(query[i]))) {
                ++i;
            }
            size_t len = i - start;
            char* term = xstrndup(query + start, len);
            if (!term) {
                return 0;
            }
            for (size_t k = 0; k < len; ++k) {
                term[k] = static_cast<char>(std::tolower(static_cast<unsigned char>(term[k])));
            }
            stem_term_inplace(term);
            if (!token_push(&raw, &raw_count, &raw_cap, Token{TOK_TERM, term})) {
                return 0;
            }
            continue;
        }
        ++i;
    }

    Token* expanded = nullptr;
    std::uint32_t exp_count = 0;
    std::uint32_t exp_cap = 0;
    for (std::uint32_t t = 0; t < raw_count; ++t) {
        if (t > 0 && is_operand_end(raw[t - 1].type) && is_operand_start(raw[t].type)) {
            if (!token_push(&expanded, &exp_count, &exp_cap, Token{TOK_AND, nullptr})) {
                return 0;
            }
        }
        if (!token_push(&expanded, &exp_count, &exp_cap, raw[t])) {
            return 0;
        }
    }

    std::free(raw);
    *out_tokens = expanded;
    *out_count = exp_count;
    return 1;
}

static int precedence(int t) {
    if (t == TOK_NOT) {
        return 3;
    }
    if (t == TOK_AND) {
        return 2;
    }
    if (t == TOK_OR) {
        return 1;
    }
    return 0;
}

static int is_right_assoc(int t) {
    return t == TOK_NOT;
}

static int to_rpn(Token* in_tokens, std::uint32_t in_count, Token** out_rpn, std::uint32_t* out_count) {
    Token* out = nullptr;
    std::uint32_t out_n = 0;
    std::uint32_t out_cap = 0;
    Token* ops = nullptr;
    std::uint32_t ops_n = 0;
    std::uint32_t ops_cap = 0;

    for (std::uint32_t i = 0; i < in_count; ++i) {
        Token t = in_tokens[i];
        if (t.type == TOK_TERM) {
            if (!token_push(&out, &out_n, &out_cap, t)) {
                return 0;
            }
            continue;
        }
        if (t.type == TOK_AND || t.type == TOK_OR || t.type == TOK_NOT) {
            while (ops_n > 0) {
                Token top = ops[ops_n - 1];
                if (!(top.type == TOK_AND || top.type == TOK_OR || top.type == TOK_NOT)) {
                    break;
                }
                int p_top = precedence(top.type);
                int p_cur = precedence(t.type);
                if (p_top > p_cur || (p_top == p_cur && !is_right_assoc(t.type))) {
                    if (!token_push(&out, &out_n, &out_cap, top)) {
                        return 0;
                    }
                    --ops_n;
                } else {
                    break;
                }
            }
            if (!token_push(&ops, &ops_n, &ops_cap, t)) {
                return 0;
            }
            continue;
        }
        if (t.type == TOK_LPAREN) {
            if (!token_push(&ops, &ops_n, &ops_cap, t)) {
                return 0;
            }
            continue;
        }
        if (t.type == TOK_RPAREN) {
            int found_lparen = 0;
            while (ops_n > 0) {
                Token top = ops[ops_n - 1];
                --ops_n;
                if (top.type == TOK_LPAREN) {
                    found_lparen = 1;
                    break;
                }
                if (!token_push(&out, &out_n, &out_cap, top)) {
                    return 0;
                }
            }
            if (!found_lparen) {
                std::free(ops);
                std::free(out);
                return 0;
            }
            continue;
        }
    }

    while (ops_n > 0) {
        Token top = ops[ops_n - 1];
        --ops_n;
        if (top.type == TOK_LPAREN || top.type == TOK_RPAREN) {
            std::free(ops);
            std::free(out);
            return 0;
        }
        if (!token_push(&out, &out_n, &out_cap, top)) {
            return 0;
        }
    }
    std::free(ops);
    *out_rpn = out;
    *out_count = out_n;
    return 1;
}

static void free_tokens(Token* tokens, std::uint32_t count) {
    if (!tokens) {
        return;
    }
    for (std::uint32_t i = 0; i < count; ++i) {
        if (tokens[i].type == TOK_TERM) {
            std::free(tokens[i].text);
        }
    }
    std::free(tokens);
}

static PostingList clone_postings(const std::uint32_t* src, std::uint32_t count) {
    PostingList out{nullptr, 0};
    if (count == 0) {
        return out;
    }
    out.ids = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * count));
    if (!out.ids) {
        return PostingList{nullptr, 0};
    }
    std::memcpy(out.ids, src, sizeof(std::uint32_t) * count);
    out.count = count;
    return out;
}

static PostingList op_and(const PostingList& a, const PostingList& b) {
    PostingList out{nullptr, 0};
    std::uint32_t max_size = (a.count < b.count) ? a.count : b.count;
    out.ids = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * max_size));
    if (!out.ids && max_size > 0) {
        return PostingList{nullptr, 0};
    }
    std::uint32_t i = 0, j = 0, k = 0;
    while (i < a.count && j < b.count) {
        if (a.ids[i] == b.ids[j]) {
            out.ids[k++] = a.ids[i];
            ++i;
            ++j;
        } else if (a.ids[i] < b.ids[j]) {
            ++i;
        } else {
            ++j;
        }
    }
    out.count = k;
    return out;
}

static PostingList op_or(const PostingList& a, const PostingList& b) {
    PostingList out{nullptr, 0};
    std::uint32_t max_size = a.count + b.count;
    out.ids = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * max_size));
    if (!out.ids && max_size > 0) {
        return PostingList{nullptr, 0};
    }
    std::uint32_t i = 0, j = 0, k = 0;
    while (i < a.count && j < b.count) {
        if (a.ids[i] == b.ids[j]) {
            out.ids[k++] = a.ids[i];
            ++i;
            ++j;
        } else if (a.ids[i] < b.ids[j]) {
            out.ids[k++] = a.ids[i++];
        } else {
            out.ids[k++] = b.ids[j++];
        }
    }
    while (i < a.count) {
        out.ids[k++] = a.ids[i++];
    }
    while (j < b.count) {
        out.ids[k++] = b.ids[j++];
    }
    out.count = k;
    return out;
}

static PostingList op_not(const IndexData* idx, const PostingList& a) {
    PostingList out{nullptr, 0};
    out.ids = static_cast<std::uint32_t*>(std::malloc(sizeof(std::uint32_t) * idx->universe_count));
    if (!out.ids && idx->universe_count > 0) {
        return PostingList{nullptr, 0};
    }
    std::uint32_t i = 0, j = 0, k = 0;
    while (i < idx->universe_count) {
        if (j >= a.count) {
            out.ids[k++] = idx->universe_ids[i++];
            continue;
        }
        if (idx->universe_ids[i] == a.ids[j]) {
            ++i;
            ++j;
        } else if (idx->universe_ids[i] < a.ids[j]) {
            out.ids[k++] = idx->universe_ids[i++];
        } else {
            ++j;
        }
    }
    out.count = k;
    return out;
}

static int posting_push(PostingList** arr, std::uint32_t* count, std::uint32_t* cap, PostingList v) {
    if (*count >= *cap) {
        std::uint32_t new_cap = (*cap == 0) ? 16 : (*cap * 2);
        PostingList* new_arr = static_cast<PostingList*>(std::realloc(*arr, sizeof(PostingList) * new_cap));
        if (!new_arr) {
            return 0;
        }
        *arr = new_arr;
        *cap = new_cap;
    }
    (*arr)[*count] = v;
    (*count)++;
    return 1;
}

static PostingList posting_pop(PostingList* arr, std::uint32_t* count) {
    PostingList empty{nullptr, 0};
    if (*count == 0) {
        return empty;
    }
    (*count)--;
    return arr[*count];
}

static PostingList eval_rpn(const IndexData* idx, Token* rpn, std::uint32_t rpn_count, int* ok) {
    PostingList* stack = nullptr;
    std::uint32_t sp = 0;
    std::uint32_t sc = 0;
    *ok = 0;

    for (std::uint32_t i = 0; i < rpn_count; ++i) {
        Token t = rpn[i];
        if (t.type == TOK_TERM) {
            std::uint64_t offset = 0;
            std::uint32_t cnt = 0;
            if (!lexicon_find(idx, t.text, &offset, &cnt)) {
                PostingList pl{nullptr, 0};
                if (!posting_push(&stack, &sp, &sc, pl)) {
                    return PostingList{nullptr, 0};
                }
            } else {
                std::uint64_t start = offset / sizeof(std::uint32_t);
                if (start + cnt > idx->postings_total) {
                    return PostingList{nullptr, 0};
                }
                PostingList pl = clone_postings(idx->postings_data + start, cnt);
                if (cnt > 0 && !pl.ids) {
                    return PostingList{nullptr, 0};
                }
                if (!posting_push(&stack, &sp, &sc, pl)) {
                    return PostingList{nullptr, 0};
                }
            }
            continue;
        }
        if (t.type == TOK_NOT) {
            if (sp < 1) {
                return PostingList{nullptr, 0};
            }
            PostingList a = posting_pop(stack, &sp);
            PostingList c = op_not(idx, a);
            std::free(a.ids);
            if (idx->universe_count > 0 && !c.ids) {
                return PostingList{nullptr, 0};
            }
            if (!posting_push(&stack, &sp, &sc, c)) {
                return PostingList{nullptr, 0};
            }
            continue;
        }
        if (t.type == TOK_AND || t.type == TOK_OR) {
            if (sp < 2) {
                return PostingList{nullptr, 0};
            }
            PostingList b = posting_pop(stack, &sp);
            PostingList a = posting_pop(stack, &sp);
            PostingList c = (t.type == TOK_AND) ? op_and(a, b) : op_or(a, b);
            std::free(a.ids);
            std::free(b.ids);
            if (((t.type == TOK_AND ? (a.count < b.count ? a.count : b.count) : (a.count + b.count)) > 0) && !c.ids) {
                return PostingList{nullptr, 0};
            }
            if (!posting_push(&stack, &sp, &sc, c)) {
                return PostingList{nullptr, 0};
            }
            continue;
        }
    }

    if (sp != 1) {
        if (stack) {
            for (std::uint32_t i = 0; i < sp; ++i) {
                std::free(stack[i].ids);
            }
            std::free(stack);
        }
        return PostingList{nullptr, 0};
    }
    PostingList out = stack[0];
    std::free(stack);
    *ok = 1;
    return out;
}

static void print_results(const IndexData* idx, const PostingList& res, std::uint32_t offset, std::uint32_t limit) {
    std::printf("TOTAL\t%u\n", res.count);
    if (offset >= res.count) {
        return;
    }
    std::uint32_t end = offset + limit;
    if (end > res.count) {
        end = res.count;
    }
    for (std::uint32_t i = offset; i < end; ++i) {
        std::uint32_t doc_id = res.ids[i];
        const char* title = "";
        const char* url = "";
        if (doc_id <= idx->max_doc_id && idx->metas_by_id[doc_id].title && idx->metas_by_id[doc_id].url) {
            title = idx->metas_by_id[doc_id].title;
            url = idx->metas_by_id[doc_id].url;
        }
        std::printf("DOC\t%u\t%s\t%s\n", doc_id, title, url);
    }
}

static int run_single_query(const IndexData* idx, const char* query, std::uint32_t offset, std::uint32_t limit) {
    Token* tokens = nullptr;
    std::uint32_t tok_count = 0;
    if (!tokenize_query(query, &tokens, &tok_count)) {
        std::fprintf(stderr, "Failed to tokenize query\n");
        return 0;
    }
    if (tok_count == 0) {
        std::printf("TOTAL\t0\n");
        std::free(tokens);
        return 1;
    }

    Token* rpn = nullptr;
    std::uint32_t rpn_count = 0;
    if (!to_rpn(tokens, tok_count, &rpn, &rpn_count)) {
        std::fprintf(stderr, "Failed to parse query\n");
        free_tokens(tokens, tok_count);
        return 0;
    }

    int ok = 0;
    PostingList result = eval_rpn(idx, rpn, rpn_count, &ok);
    if (!ok) {
        std::fprintf(stderr, "Failed to evaluate query\n");
        std::free(rpn);
        free_tokens(tokens, tok_count);
        return 0;
    }

    print_results(idx, result, offset, limit);
    std::free(result.ids);
    std::free(rpn);
    free_tokens(tokens, tok_count);
    return 1;
}

int main(int argc, char** argv) {
    const char* index_dir = nullptr;
    const char* query = nullptr;
    std::uint32_t offset = 0;
    std::uint32_t limit = 50;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--index-dir") == 0 && i + 1 < argc) {
            index_dir = argv[++i];
        } else if (std::strcmp(argv[i], "--query") == 0 && i + 1 < argc) {
            query = argv[++i];
        } else if (std::strcmp(argv[i], "--offset") == 0 && i + 1 < argc) {
            offset = static_cast<std::uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            limit = static_cast<std::uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        }
    }

    if (!index_dir) {
        std::fprintf(stderr, "Usage: search_cli --index-dir <dir> [--query q] [--offset n] [--limit n]\n");
        return 1;
    }

    char* postings_path = path_join3(index_dir, "postings.bin");
    char* lexicon_path = path_join3(index_dir, "lexicon.bin");
    char* forward_path = path_join3(index_dir, "forward.bin");
    if (!postings_path || !lexicon_path || !forward_path) {
        std::fprintf(stderr, "Failed to allocate index paths\n");
        std::free(postings_path);
        std::free(lexicon_path);
        std::free(forward_path);
        return 1;
    }

    IndexData idx{};
    if (!load_postings(&idx, postings_path) || !load_lexicon(&idx, lexicon_path) || !load_forward(&idx, forward_path)) {
        std::fprintf(stderr, "Failed to load index files\n");
        std::free(postings_path);
        std::free(lexicon_path);
        std::free(forward_path);
        free_index(&idx);
        return 1;
    }
    std::free(postings_path);
    std::free(lexicon_path);
    std::free(forward_path);

    int ok = 1;
    if (query) {
        ok = run_single_query(&idx, query, offset, limit);
    } else {
        char line[4096];
        while (std::fgets(line, sizeof(line), stdin)) {
            size_t n = std::strlen(line);
            while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
                line[n - 1] = '\0';
                --n;
            }
            if (line[0] == '\0') {
                continue;
            }
            std::printf("QUERY\t%s\n", line);
            if (!run_single_query(&idx, line, offset, limit)) {
                ok = 0;
                break;
            }
            std::printf("\n");
        }
    }

    free_index(&idx);
    return ok ? 0 : 1;
}
