#include "gtstore.hpp"
#include "utils.hpp"

#include <algorithm>
#include <functional>
#include <sstream>
#include <unordered_set>

using namespace gtstore_utils;
using namespace std;

GTStoreClient::GTStoreClient() {
	manager_address.host = DEFAULT_MANAGER_HOST;
	manager_address.port = DEFAULT_MANAGER_PORT;
	replication_factor = 0;
}

// This picks a storage node based on key hash.
StorageNodeInfo GTStoreClient::pick_primary(const string &key) {
	return pick_node_for_attempt(key, 0);
}

// This picks the Nth replica for the hash.
StorageNodeInfo GTStoreClient::pick_node_for_attempt(const string &key, size_t attempt) {
	if (routing_table.empty()) {
		return StorageNodeInfo{"", {DEFAULT_MANAGER_HOST, DEFAULT_STORAGE_BASE_PORT}, 0};
	}
	uint64_t hash_value = consistent_hash(key);
	size_t start_index = routing_table.size();
	for (size_t i = 0; i < routing_table.size(); ++i) {
		if (hash_value <= routing_table[i].token) {
			start_index = i;
			break;
		}
	}
	if (start_index == routing_table.size()) {
		start_index = 0; // wrap around
	}
	
	// walk forward and collect distinct physical nodes in order
	unordered_set<string> used_physical_nodes;
	vector<StorageNodeInfo> unique_nodes;
	size_t steps = 0;
	size_t index = start_index;
	while (steps < routing_table.size() && unique_nodes.size() <= attempt) {
		const auto &candidate = routing_table[index];
		if (used_physical_nodes.insert(candidate.node_id).second) {
			unique_nodes.push_back(candidate);
			if (unique_nodes.size() > attempt) {
				break; // we already have the requested attempt slot
			}
		}
		index = (index + 1) % routing_table.size();
		steps++;
	}
	if (unique_nodes.empty()) {
		return routing_table[start_index];
	}
	if (attempt >= unique_nodes.size()) {
		return unique_nodes.back();
	}
	return unique_nodes[attempt];
}

// This turns payload into value list.
val_t GTStoreClient::parse_value(const string &payload) {
	val_t parts;
	auto strings = split(payload, ',');
	for (auto &entry : strings) {
		if (!entry.empty()) {
			parts.push_back(entry);
		}
	}
	return parts;
}

// This turns value list into string.
string GTStoreClient::serialize_value(const val_t &value) {
	return join(value, ',');
}

// This refreshes the routing table from manager.
bool GTStoreClient::refresh_table() {
	int fd = connect_to_host(manager_address);
	if (fd < 0) {
		log_line("ERROR", "failed to reach manager for refresh");
		return false;
	}
	if (!send_message(fd, MessageType::CLIENT_HELLO, "")) {
		log_line("ERROR", "could not send hello");
		close(fd);
		return false;
	}
	MessageType type;
	string payload;
	if (!recv_message(fd, type, payload)) {
		log_line("ERROR", "no table from manager");
		close(fd);
		return false;
	}
	close(fd);
	if (type != MessageType::TABLE_PUSH) {
		log_line("WARN", "manager replied without table");
		return false;
	}
	size_t parsed_factor;
	routing_table = parse_table_payload(payload, parsed_factor);
	replication_factor = max<size_t>(1, parsed_factor);
	log_line("INFO", "Routing table now has " + to_string(routing_table.size()) + " nodes with replication factor " + to_string(replication_factor));
	log_line("INFO", "Routing table detail: " + describe_table(routing_table));
	return !routing_table.empty();
}

// This verifies the key size.
bool GTStoreClient::validate_key(const string &key) {
	if (key.empty()) {
		log_line("WARN", "key is empty");
		return false;
	}
	if (key.size() > MAX_KEY_BYTE_PER_REQUEST) {
		log_line("WARN", "key too large");
		return false;
	}
	return true;
}

// This verifies the value size.
bool GTStoreClient::validate_value(const val_t &value) {
	size_t total = 0;
	for (size_t i = 0; i < value.size(); ++i) {
		total += value[i].size();
		if (i + 1 < value.size()) {
			total += 1; // comma separator
		}
	}
	if (total > MAX_VALUE_BYTE_PER_REQUEST) {
		log_line("WARN", "value too large");
		return false;
	}
	return true;
}

// This connects to manager and learns routing table.
void GTStoreClient::init(int id, const string &manager_host, uint16_t manager_port) {

		cout << "Inside GTStoreClient::init() for client " << id << "\n";
		client_id = id;
		setup_logging("client_" + to_string(client_id));
		manager_address = {manager_host, manager_port};
		if (!refresh_table()) {
			log_line("WARN", "client has empty routing table");
		}
}

// This asks storage node for a key.
val_t GTStoreClient::get(string key) {
		cout << "Inside GTStoreClient::get() for client: " << client_id << " key: " << key << "\n";
		val_t value = {};
		if (!validate_key(key)) {
			return value;
		}
		size_t available_nodes = routing_table.size();
		size_t max_attempts = min(replication_factor, max<size_t>(1, available_nodes));

		if (max_attempts == 0) {
			if (!refresh_table()) {
				log_line("ERROR", "get failed: no routing info");
				return value;
			}
			available_nodes = routing_table.size();
			max_attempts = min(replication_factor, max<size_t>(1, available_nodes));
			if (max_attempts == 0) {
				return value;
			}
		}

		for (size_t attempt = 0; attempt < max_attempts; ++attempt) {
			StorageNodeInfo node = pick_node_for_attempt(key, attempt);
			if (node.node_id.empty()) {
				if (!refresh_table()) {
					break;
				}
				continue;
			}
			log_line("INFO", "get attempt key=" + key + " target=" + node.node_id);
			int fd = connect_to_host(node.address);
			if (fd < 0) {
				log_line("ERROR", "get connect failed for " + node.node_id);
				refresh_table();
				continue;
			}
			if (!send_message(fd, MessageType::CLIENT_GET, key)) {
				log_line("ERROR", "get send failed");
				close(fd);
				refresh_table();
				continue;
			}
			MessageType type;
			string payload;
			bool ok = recv_message(fd, type, payload);
			close(fd);
			if (ok && type == MessageType::GET_OK) {
				value = parse_value(payload);
				log_line("INFO", "get success key=" + key + " value=" + payload + " from=" + node.node_id);
				cout << key << ", " << payload << ", " << node.node_id << endl;
				return value;
			}
			refresh_table();
		}
		log_line("WARN", "get failed after retries");
		return value;
}

// This sends a value update to storage node.
bool GTStoreClient::put(string key, val_t value) {

		string print_value = "";
		for (uint i = 0; i < value.size(); i++) {
				print_value += value[i] + " ";
		}
		cout << "Inside GTStoreClient::put() for client: " << client_id << " key: " << key << " value: " << print_value << "\n";
		if (!validate_key(key) || !validate_value(value)) {
			log_line("ERROR", "put failed: invalid key/value size");
			return false;
		}
		string payload = key + "|" + serialize_value(value);
		size_t available_nodes = routing_table.size();
		size_t replicas = min(replication_factor, max<size_t>(1, available_nodes));
		if (replicas == 0) {
			if (!refresh_table()) {
				log_line("ERROR", "put failed: no routing info");
				return false;
			}
			available_nodes = routing_table.size();
			replicas = min(replication_factor, max<size_t>(1, available_nodes));
			if (replicas == 0) {
				return false;
			}
		}
		
		// try all K replicas until one succeeds (that node becomes primary for chain replication)
		for (size_t attempt = 0; attempt < replicas; ++attempt) {
			StorageNodeInfo node = pick_node_for_attempt(key, attempt);
			if (node.node_id.empty()) {
				if (!refresh_table()) {
					break;
				}
				continue;
			}
			
			size_t sep_pos = payload.find('|');
			string value_slice = (sep_pos == string::npos) ? payload : payload.substr(sep_pos + 1);
			log_line("INFO", "put attempt key=" + key + " value=" + value_slice + " target=" + node.node_id);
			int fd = connect_to_host(node.address);
			if (fd < 0) {
				log_line("ERROR", "put connect failed for " + node.node_id);
				refresh_table();
				continue;
			}
			
			bool ok = send_message(fd, MessageType::CLIENT_PUT, payload);
			MessageType type;
			string resp;
			bool ack = ok && recv_message(fd, type, resp) && type == MessageType::PUT_OK;
			close(fd);

			if (ack) {
				cout << "OK, " << node.node_id << endl;
				log_line("INFO", "put success key=" + key + " stored_on=" + node.node_id);
				return true;
			} else {
				log_line("WARN", "put failed on " + node.node_id + ", trying next replica");
				refresh_table();
			}
		}
		
		log_line("ERROR", "put failed on " + to_string(replicas) + " replicas");
		return false;
}

// This closes client side work.
void GTStoreClient::finalize() {

		cout << "Inside GTStoreClient::finalize() for client " << client_id << "\n";
		log_line("INFO", "client finalize called");

		//note: no cleanup is done in this implementation....
}

// This returns the current routing table snapshot.
vector<StorageNodeInfo> GTStoreClient::current_table_snapshot() const {
	return routing_table;
}

// This exposes the routing pick logic for tests.
StorageNodeInfo GTStoreClient::debug_pick_for_test(const string &key, size_t attempt) {
	return pick_node_for_attempt(key, attempt);
}

// This returns the last known replication factor.
size_t GTStoreClient::current_replication() const {
	return replication_factor;
}
