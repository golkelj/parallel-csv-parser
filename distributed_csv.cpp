// distributed_csv.cpp
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cmath>
#include <limits>
#include <iomanip>
#include <cctype>

using namespace std;

struct Welford {
    uint64_t n = 0;
    long double mean = 0;
    long double M2 = 0; 

    void add(long double x) {
        n++;
        long double delta = x - mean;
        mean += delta / n;
        long double delta2 = x - mean;
        M2 += delta * delta2;
    }

    long double variance() const { return n > 1 ? M2 / (n - 1) : 0.0L; }
    long double stddev() const { return sqrt(variance()); }

    void merge(const Welford& other) {
        if (other.n == 0) return;
        if (n == 0) { *this = other; return; }

        long double delta = other.mean - mean;
        long double total_n = (long double)(n + other.n);
        long double new_mean = (mean * n + other.mean * other.n) / total_n;
        long double new_M2 = M2 + other.M2 + delta * delta * (long double)n * (long double)other.n / total_n;

        mean = new_mean;
        M2 = new_M2;
        n += other.n;
    }
};

struct ColStats {
    uint64_t count = 0; 
    uint64_t null_count = 0;
    uint64_t non_numeric = 0;
    long double sum = 0;
    long double min_val = numeric_limits<long double>::infinity();
    long double max_val = -numeric_limits<long double>::infinity();
    Welford w;

    void add_numeric(long double x) {
        count++;
        sum += x;
        if (x < min_val) min_val = x;
        if (x > max_val) max_val = x;
        w.add(x);
    }

    void merge(const ColStats &other) {
        count += other.count;
        null_count += other.null_count;
        non_numeric += other.non_numeric;
        sum += other.sum;
        if (other.count > 0) {
            if (min_val > other.min_val) min_val = other.min_val;
            if (max_val < other.max_val) max_val = other.max_val;
        }
        w.merge(other.w);
    }
};

// csv parsing
vector<string> parse_csv_line(const string &line) {
    vector<string> out;
    string cur;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i+1] == '"') { cur.push_back('"'); ++i; }
                else in_quotes = false;
            } else cur.push_back(c);
        } else {
            if (c == '"') in_quotes = true;
            else if (c == ',') { out.push_back(cur); cur.clear(); }
            else cur.push_back(c);
        }
    }
    out.push_back(cur);
    return out;
}

bool is_number(const string &s, long double &out) {
    string t;
    size_t i = 0, j = s.size();
    while (i < j && isspace((unsigned char)s[i])) ++i;
    while (j > i && isspace((unsigned char)s[j-1])) --j;
    if (i >= j) return false;
    t = s.substr(i, j-i);
    // try stold
    try {
        size_t idx = 0;
        out = stold(t, &idx);
        return idx == t.size();
    } catch (...) {
        return false;
    }
}

struct PartialResult {
    vector<ColStats> cols;
    uint64_t rows_processed = 0;
    string worker_id;
};

PartialResult process_chunk(const vector<string> &lines, size_t ncols, const string &worker_id) {
    PartialResult pr;
    pr.cols.resize(ncols);
    pr.worker_id = worker_id;
    bool header_skipped = false;

    for (const string &line : lines) {
        auto cells = parse_csv_line(line);
        for (size_t c = 0; c < ncols; ++c) {
            string cell = c < cells.size() ? cells[c] : "";
            if (cell.empty()) { pr.cols[c].null_count++; continue; }
            long double val;
            if (is_number(cell, val)) {
                pr.cols[c].add_numeric(val);
            } else {
                pr.cols[c].non_numeric++;
            }
        }
        pr.rows_processed++;
    }
    return pr;
}

PartialResult merge_partials(const vector<PartialResult> &partials) {
    if (partials.empty()) return PartialResult();
    size_t ncols = partials[0].cols.size();
    PartialResult agg;
    agg.cols.resize(ncols);
    agg.rows_processed = 0;
    for (const auto &p : partials) {
        for (size_t i = 0; i < ncols; ++i) agg.cols[i].merge(p.cols[i]);
        agg.rows_processed += p.rows_processed;
    }
    return agg;
}

// cli main part
int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <csv-file> <num-workers>\n";
        return 1;
    }
    std::string filename = argv[1];
    int workers = stoi(argv[2]);
    if (workers <= 0) workers = 1;

    ifstream in(filename);
    if (!in.is_open()) { 
        cerr << "Failed to open " << filename << "\n"; 
        return 1; 
    }

    std::string header;
    if (!getline(in, header)) { 
        cerr << "Empty file\n"; 
        return 1; 
    }
    auto header_cells = parse_csv_line(header);
    size_t ncols = header_cells.size();

    // Read remainder of file lines into a vector (streaming version could chunk by bytes)
    vector<string> all_lines;
    string line;
    while (getline(in, line)) all_lines.push_back(line);

    // Split into roughly equal chunks by line count
    vector<vector<string>> chunks(workers);
    for (size_t i = 0; i < all_lines.size(); ++i) {
        chunks[i % workers].push_back(all_lines[i]);
    }

    // Launch worker threads
    vector<PartialResult> partials(workers);
    vector<thread> ths;
    mutex m;
    for (int w = 0; w < workers; ++w) {
        ths.emplace_back([w, &chunks, ncols, &partials, &m]() {
            string wid = "worker-" + to_string(w);
            auto res = process_chunk(chunks[w], ncols, wid);
            // store result (thread-safe)
            lock_guard<mutex> lk(m);
            partials[w] = std::move(res);
        });
    }

    for (auto &t : ths) t.join();

    PartialResult final_res = merge_partials(partials);
    cout << "Aggregated rows: " << final_res.rows_processed << "\n";
    cout << "Column summaries:\n";
    cout.setf(std::ios::fixed); cout<<setprecision(6);
    for (size_t c = 0; c < ncols; ++c) {
        auto &s = final_res.cols[c];
        cout << "  [" << c << "] " << header_cells[c] << " :\n";
        cout << "     numeric_count=" << s.count << ", nulls=" << s.null_count << ", non_numeric=" << s.non_numeric << "\n";
        if (s.count > 0) {
            cout << "     sum=" << (double)s.sum << ", min=" << (double)s.min_val << ", max=" << (double)s.max_val
                 << ", mean=" << (double)s.w.mean << ", std=" << (double)s.w.stddev() << "\n";
        }
    }
    return 0;
}
