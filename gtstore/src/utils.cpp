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

namespace gtstore_utils {

namespace {
std::ofstream log_stream;
std::string current_component;

std::string timestamp() {
    std::time_t now = std::time(nullptr);
    std::tm *tm_now = std::localtime(&now);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm_now);
    return std::string(buffer);
}
}

// This sets up logging for the process.
void setup_logging(const std::string &component_name) {
    current_component = component_name;
    mkdir("logs", 0755);
    std::string file_name = "logs/" + component_name + ".log";
    if (log_stream.is_open()) {
        log_stream.close();
    }
    log_stream.open(file_name.c_str(), std::ios::app);
    log_line("INFO", "log started");
}

// This prints and writes a log line.
void log_line(const std::string &level, const std::string &message) {
    std::string line = "[" + timestamp() + "][" + current_component + "][" + level + "] " + message;
    std::cout << line << std::endl;
    if (log_stream.is_open()) {
        log_stream << line << std::endl;
        log_stream.flush();
    }
}

// This splits a string by the delimiter.
std::vector<std::string> split(const std::string &input, char delimiter) {
    std::vector<std::string> parts;
    std::string current;
    std::istringstream stream(input);
    while (std::getline(stream, current, delimiter)) {
        parts.push_back(current);
    }
    return parts;
}

// This joins strings with the delimiter.
std::string join(const std::vector<std::string> &parts, char delimiter) {
    std::ostringstream builder;
    for (size_t i = 0; i < parts.size(); ++i) {
        builder << parts[i];
        if (i + 1 < parts.size()) {
            builder << delimiter;
        }
    }
    return builder.str();
}

// This trims whitespace from both ends.
std::string trim(const std::string &value) {
    size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }
    size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(start, end - start);
}

// This converts the storage table to a payload string.
std::string build_table_payload(const std::vector<StorageNodeInfo> &nodes, size_t replication_factor) {
    std::vector<std::string> rows;
    for (const auto &node : nodes) {
        std::ostringstream row;
        row << node.node_id << "," << node.address.host << "," << node.address.port << "," << node.token;
        rows.push_back(row.str());
    }
    std::string table = join(rows, ';');
    return std::to_string(replication_factor) + "#" + table;
}

// This parses a payload back into storage entries.
std::vector<StorageNodeInfo> parse_table_payload(const std::string &payload, size_t &replication_factor) {
    std::vector<StorageNodeInfo> result;
    replication_factor = 1;
    std::string table_section = payload;
    auto hash_pos = payload.find('#');
    if (hash_pos != std::string::npos) {
        std::string prefix = trim(payload.substr(0, hash_pos));
        if (!prefix.empty()) {
            try {
                replication_factor = static_cast<size_t>(std::stoul(prefix));
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
        info.address.port = static_cast<uint16_t>(std::stoi(cols[2]));
        info.token = static_cast<uint64_t>(std::stoull(cols[3]));
        result.push_back(info);
    }
    return result;
}

// This reads a port value from argv.
uint16_t read_port_from_arg(int argc, char **argv, uint16_t default_port) {
    if (argc < 2) {
        return default_port;
    }
    int value = std::atoi(argv[1]);
    if (value <= 0 || value > 65535) {
        return default_port;
    }
    return static_cast<uint16_t>(value);
}

}
