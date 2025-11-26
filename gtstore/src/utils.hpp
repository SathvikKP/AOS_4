#ifndef GTSTORE_UTILS_HPP
#define GTSTORE_UTILS_HPP

#include <string>
#include <vector>

#include "net_common.hpp"

using namespace std;

namespace gtstore_utils {

// This sets up logging for the process.
void setup_logging(const string &component_name);

// This prints and writes a log line.
void log_line(const string &level, const string &message);

// This splits a string by the delimiter.
vector<string> split(const string &input, char delimiter);

// This joins strings with the delimiter.
string join(const vector<string> &parts, char delimiter);

// This trims whitespace from both ends.
string trim(const string &value);

// This converts the storage table plus replication factor to a payload string.
string build_table_payload(const vector<StorageNodeInfo> &nodes, size_t replication_factor);

// This parses a payload back into storage entries and extracts replication factor.
vector<StorageNodeInfo> parse_table_payload(const string &payload, size_t &replication_factor);

// This generates virtual tokens for a physical node.
vector<uint64_t> generate_virtual_tokens(const string &physical_node_id, int num_vnodes);

// This computes a consistent hash for a key (used for both virtual tokens and key hashing).
uint64_t consistent_hash(const string &input);

// This reads a port value from argv.
uint16_t read_port_from_arg(int argc, char **argv, uint16_t default_port);

// This creates a readable string description of the routing table.
string describe_table(const vector<StorageNodeInfo> &nodes);

}

#endif
