#include <chrono>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

static void split_tsv_5(const std::string& line, std::string& c1, std::string& c2, std::string& c3,
                        std::string& c4, std::string& c5) {
    size_t p1 = line.find('\t');
    if (p1 == std::string::npos) {
        c1 = line;
        c2.clear();
        c3.clear();
        c4.clear();
        c5.clear();
        return;
    }
    size_t p2 = line.find('\t', p1 + 1);
    size_t p3 = (p2 == std::string::npos) ? std::string::npos : line.find('\t', p2 + 1);
    size_t p4 = (p3 == std::string::npos) ? std::string::npos : line.find('\t', p3 + 1);

    c1 = line.substr(0, p1);
    c2 = (p2 == std::string::npos) ? "" : line.substr(p1 + 1, p2 - p1 - 1);
    c3 = (p3 == std::string::npos) ? "" : line.substr(p2 + 1, p3 - p2 - 1);
    c4 = (p4 == std::string::npos) ? "" : line.substr(p3 + 1, p4 - p3 - 1);
    c5 = (p4 == std::string::npos) ? "" : line.substr(p4 + 1);
}

static std::vector<std::string> tokenize_text(const std::string& text) {
    std::vector<std::string> tokens;
    std::string current;
    current.reserve(32);

    for (unsigned char ch : text) {
        if (std::isalnum(ch)) {
            current.push_back(static_cast<char>(std::tolower(ch)));
        } else if (!current.empty()) {
            tokens.push_back(current);
            current.clear();
        }
    }
    if (!current.empty()) {
        tokens.push_back(current);
    }
    return tokens;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: tokenizer <raw_text.tsv> <tokenized.txt>\n";
        return 1;
    }

    const std::string input_path = argv[1];
    const std::string output_path = argv[2];

    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open input: " << input_path << "\n";
        return 1;
    }
    std::ofstream out(output_path);
    if (!out) {
        std::cerr << "Failed to open output: " << output_path << "\n";
        return 1;
    }

    auto started = std::chrono::steady_clock::now();

    std::string line;
    std::uint64_t doc_count = 0;
    std::uint64_t token_count = 0;
    std::uint64_t token_length_sum = 0;
    std::uint64_t input_bytes = 0;

    while (std::getline(in, line)) {
        input_bytes += static_cast<std::uint64_t>(line.size() + 1);
        if (line.empty()) {
            continue;
        }

        std::string doc_id, source, url, title, text;
        split_tsv_5(line, doc_id, source, url, title, text);
        if (doc_id.empty() || text.empty()) {
            continue;
        }

        std::vector<std::string> tokens = tokenize_text(text);
        if (tokens.empty()) {
            continue;
        }

        ++doc_count;
        out << doc_id << '\t';
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (i > 0) {
                out << ' ';
            }
            out << tokens[i];
            ++token_count;
            token_length_sum += static_cast<std::uint64_t>(tokens[i].size());
        }
        out << '\n';
    }

    auto ended = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(ended - started).count();
    double avg_len = token_count == 0 ? 0.0 : static_cast<double>(token_length_sum) / static_cast<double>(token_count);
    double kb = static_cast<double>(input_bytes) / 1024.0;
    double sec_per_kb = kb > 0.0 ? elapsed_sec / kb : 0.0;

    std::cout << "Tokenizer finished\n";
    std::cout << "documents=" << doc_count << "\n";
    std::cout << "tokens=" << token_count << "\n";
    std::cout << "avg_token_length=" << avg_len << "\n";
    std::cout << "elapsed_seconds=" << elapsed_sec << "\n";
    std::cout << "seconds_per_kb=" << sec_per_kb << "\n";

    return 0;
}
