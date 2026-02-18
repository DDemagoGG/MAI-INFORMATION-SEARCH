#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static int read_line(FILE* in, char** buffer, size_t* capacity) {
    if (!in || !buffer || !capacity) {
        return -1;
    }
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

static int ends_with(const char* s, int n, const char* suffix) {
    int m = static_cast<int>(std::strlen(suffix));
    if (n < m) {
        return 0;
    }
    return std::memcmp(s + n - m, suffix, static_cast<size_t>(m)) == 0;
}

static void stem_token(char* token) {
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

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: stemmer <tokenized.txt> <stemmed.txt>\n");
        return 1;
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

    char* line = nullptr;
    size_t line_cap = 0;
    std::uint64_t docs = 0;
    std::uint64_t tokens = 0;

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
        *tab = '\0';
        const char* doc_id = line;
        char* body = tab + 1;

        std::fprintf(out, "%s\t", doc_id);
        int first = 1;

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

            stem_token(start);
            if (start[0] != '\0') {
                if (!first) {
                    std::fputc(' ', out);
                }
                std::fputs(start, out);
                first = 0;
                ++tokens;
            }

            if (!saved) {
                break;
            }
            *p = saved;
        }
        std::fputc('\n', out);
        ++docs;
    }

    std::free(line);
    std::fclose(in);
    std::fclose(out);

    std::printf("Stemmer finished\n");
    std::printf("documents=%llu\n", static_cast<unsigned long long>(docs));
    std::printf("tokens=%llu\n", static_cast<unsigned long long>(tokens));
    return 0;
}
