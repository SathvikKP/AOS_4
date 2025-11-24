#ifndef GTSTORE_UTILS_HPP
#define GTSTORE_UTILS_HPP

#include <string>
#include <vector>

#include "net_common.hpp"

namespace gtstore_utils {

// This sets up logging for the process.
void setup_logging(const std::string &component_name);

// This prints and writes a log line.
void log_line(const std::string &level, const std::string &message);

// This splits a string by the delimiter.
std::vector<std::string> split(const std::string &input, char delimiter);

// This joins strings with the delimiter.
std::string join(const std::vector<std::string> &parts, char delimiter);

// This trims whitespace from both ends.
std::string trim(const std::string &value);

// This converts the storage table plus replication factor to a payload string.
std::string build_table_payload(const std::vector<StorageNodeInfo> &nodes, size_t replication_factor);

// This parses a payload back into storage entries and extracts replication factor.
std::vector<StorageNodeInfo> parse_table_payload(const std::string &payload, size_t &replication_factor);

// This reads a port value from argv.
uint16_t read_port_from_arg(int argc, char **argv, uint16_t default_port);

// This creates a readable string description of the routing table.
std::string describe_table(const std::vector<StorageNodeInfo> &nodes);

}

#endif
