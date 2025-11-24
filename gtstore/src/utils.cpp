#include "utils.hpp"

#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

namespace gtstore_utils {

namespace {
ofstream log_stream;
string current_component;

string timestamp() {
    time_t now = time(nullptr);
    tm *tm_now = localtime(&now);
    char buffer[32];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm_now);
    return string(buffer);
}
}

// This sets up logging for the process.
void setup_logging(const string &component_name) {
    current_component = component_name;
    mkdir("logs", 0755);
    string file_name = "logs/" + component_name + ".log";
    if (log_stream.is_open()) {
        log_stream.close();
    }
    log_stream.open(file_name.c_str(), ios::app);
    log_line("INFO", "log started");
}

// This prints and writes a log line.
void log_line(const string &level, const string &message) {
    string line = "[" + timestamp() + "][" + current_component + "][" + level + "] " + message;
    cout << line << endl;
    if (log_stream.is_open()) {
        log_stream << line << endl;
        log_stream.flush();
    }
}

// This splits a string by the delimiter.
vector<string> split(const string &input, char delimiter) {
    vector<string> parts;
    string current;
    istringstream stream(input);
    while (getline(stream, current, delimiter)) {
        parts.push_back(current);
    }
    return parts;
}

// This joins strings with the delimiter.
string join(const vector<string> &parts, char delimiter) {
    ostringstream builder;
    for (size_t i = 0; i < parts.size(); ++i) {
        builder << parts[i];
        if (i + 1 < parts.size()) {
            builder << delimiter;
        }
    }
    return builder.str();
}

// This trims whitespace from both ends.
string trim(const string &value) {
    size_t start = 0;
    while (start < value.size() && isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }
    size_t end = value.size();
    while (end > start && isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(start, end - start);
}

// This converts the storage table to a payload string.
string build_table_payload(const vector<StorageNodeInfo> &nodes, size_t replication_factor) {
    vector<string> rows;
    for (const auto &node : nodes) {
        ostringstream row;
        row << node.node_id << "," << node.address.host << "," << node.address.port << "," << node.token;
        rows.push_back(row.str());
    }
    string table = join(rows, ';');
    return to_string(replication_factor) + "#" + table;
}

// This parses a payload back into storage entries.
vector<StorageNodeInfo> parse_table_payload(const string &payload, size_t &replication_factor) {
    vector<StorageNodeInfo> result;
    replication_factor = 1;
    string table_section = payload;
    auto hash_pos = payload.find('#');
    if (hash_pos != string::npos) {
        string prefix = trim(payload.substr(0, hash_pos));
        if (!prefix.empty()) {
            try {
                replication_factor = static_cast<size_t>(stoul(prefix));
            } catch (...) {
                replication_factor = 1;
            }
        }
        table_section = payload.substr(hash_pos + 1);
    }
    auto rows = split(table_section, ';');
    for (const auto &row : rows) {
        if (row.empty()) {
            continue;
        }
        auto cols = split(row, ',');
        if (cols.size() != 4) {
            continue;
        }
        StorageNodeInfo info;
        info.node_id = trim(cols[0]);
        info.address.host = trim(cols[1]);
        info.address.port = static_cast<uint16_t>(stoi(cols[2]));
        info.token = static_cast<uint64_t>(stoull(cols[3]));
        result.push_back(info);
    }
    return result;
}

// This reads a port value from argv.
uint16_t read_port_from_arg(int argc, char **argv, uint16_t default_port) { // TODO: add here more parameters?
    if (argc < 2) {
        return default_port;
    }
    int value = atoi(argv[1]);
    if (value <= 0 || value > 65535) {
        return default_port;
    }
    return static_cast<uint16_t>(value);
}

string describe_table(const vector<StorageNodeInfo> &nodes) {
    if (nodes.empty()) {
        return "<empty>";
    }
    ostringstream out;
    for (size_t i = 0; i < nodes.size(); ++i) {
        out << nodes[i].node_id << "@" << nodes[i].address.host << ":" << nodes[i].address.port
            << " token=" << nodes[i].token;
        if (i + 1 < nodes.size()) {
            out << " | ";
        }
    }
    return out.str();
}

}
