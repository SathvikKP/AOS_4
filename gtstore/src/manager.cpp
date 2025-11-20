#include "gtstore.hpp"
#include "utils.hpp"

#include <algorithm>
#include <functional>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <sstream>

using namespace gtstore_utils;

namespace {
const std::string COMPONENT_NAME = "manager";
const int BACKLOG = 16;

// This formats the routing table for logging.
std::string describe_nodes(const std::vector<StorageNodeInfo> &nodes) {
	if (nodes.empty()) {
		return "<empty>";
	}
	std::ostringstream out;
	for (size_t i = 0; i < nodes.size(); ++i) {
		out << nodes[i].node_id << "@" << nodes[i].address.host << ":" << nodes[i].address.port
		    << " token=" << nodes[i].token;
		if (i + 1 < nodes.size()) {
			out << " | ";
		}
	}
	return out.str();
}

bool send_table(int client_fd, const std::vector<StorageNodeInfo> &nodes, size_t replication_factor) {
	log_line("INFO", "Sending routing table (rep=" + std::to_string(replication_factor) + "): " + describe_nodes(nodes));
	std::string payload = build_table_payload(nodes, replication_factor);
	return send_message(client_fd, MessageType::TABLE_PUSH, payload);
}
}

// This starts the manager listener.
void GTStoreManager::init() {
	setup_logging(COMPONENT_NAME);
	log_line("INFO", "Inside GTStoreManager::init()");
	listen_port = DEFAULT_MANAGER_PORT;
	replication_factor = 2;
	running = true;
	const char *env = std::getenv("GTSTORE_REPL");
	if (env) {
		int parsed = std::atoi(env);
		if (parsed >= 1) {
			replication_factor = static_cast<size_t>(parsed);
		}
	}
	setup_logging(COMPONENT_NAME);
	log_line("INFO", "Replication factor set to " + std::to_string(replication_factor));
	NodeAddress addr{DEFAULT_MANAGER_HOST, listen_port};
	listen_fd = create_listen_socket(addr, BACKLOG);
	if (listen_fd < 0) {
		log_line("ERROR", "Failed to create manager listen socket");
		return;
	}
	log_line("INFO", "Manager listening on " + addr.host + ":" + std::to_string(addr.port));
	heartbeat_thread = std::thread(&GTStoreManager::monitor_heartbeats, this);
	heartbeat_thread.detach();
	accept_loop();
}

// This accepts client and storage sockets.
void GTStoreManager::accept_loop() {
	while (true) {
		int client_fd = accept_client(listen_fd);
		if (client_fd < 0) {
			continue;
		}
		std::thread([this, client_fd]() {
			MessageType type;
			std::string payload;
			if (!recv_message(client_fd, type, payload)) {
				close(client_fd);
				return;
			}
				switch (type) {
					case MessageType::STORAGE_REGISTER:
						handle_storage_register(payload);
						send_table(client_fd, snapshot_nodes(), replication_factor);
						break;
					case MessageType::CLIENT_HELLO:
						log_line("INFO", "Client requested table");
						send_table(client_fd, snapshot_nodes(), replication_factor);
						break;
					case MessageType::HEARTBEAT:
						handle_heartbeat(payload);
						send_message(client_fd, MessageType::HEARTBEAT_ACK, "ok");
						break;
					default:
						log_line("WARN", "Unknown message type received");
			}
			close(client_fd);
		}).detach();
	}
}

// This records a storage registration.
void GTStoreManager::handle_storage_register(const std::string &payload) {
	auto parts = gtstore_utils::split(payload, ',');
	if (parts.size() != 3) {
		log_line("WARN", "Invalid storage registration payload");
		return;
	}
	StorageNodeInfo info;
	info.node_id = parts[0];
	info.address.host = parts[1];
	info.address.port = static_cast<uint16_t>(std::stoi(parts[2]));
	std::hash<std::string> hasher;
	std::string token_seed = info.node_id + "-" + info.address.host + ":" + std::to_string(info.address.port);
	info.token = static_cast<uint64_t>(hasher(token_seed));
	{
		std::lock_guard<std::mutex> guard(table_mutex);
		auto existing = std::find_if(node_table.begin(), node_table.end(), [&](const StorageNodeInfo &node) {
			return node.node_id == info.node_id;
		});
		if (existing != node_table.end()) {
			*existing = info;
		} else {
			node_table.push_back(info);
		}
		heartbeat_times[info.node_id] = std::chrono::steady_clock::now(); // what if same node.id?
		std::sort(node_table.begin(), node_table.end(), [](const StorageNodeInfo &lhs, const StorageNodeInfo &rhs) {
			return lhs.token < rhs.token;
		});
	}
	log_line("INFO", "Registered storage " + info.node_id + " at " + info.address.host + ":" + std::to_string(info.address.port));
	log_line("INFO", "Routing table snapshot: " + describe_nodes(snapshot_nodes()));
	
	// Trigger rebalancing for the new node
	int new_idx = find_node_index(info.node_id);
	if (new_idx >= 0 && node_table.size() > 1) {
		// Only rebalance if there are other nodes
		rebalance_on_node_join(new_idx, info.token);
	}
}

// This records heartbeat timestamps.
void GTStoreManager::handle_heartbeat(const std::string &payload) {
	std::lock_guard<std::mutex> guard(table_mutex);
	heartbeat_times[payload] = std::chrono::steady_clock::now();
}

// This copies current node table.
std::vector<StorageNodeInfo> GTStoreManager::snapshot_nodes() {
	std::lock_guard<std::mutex> guard(table_mutex);
	return node_table;
}

// Find index of node by node_id
int GTStoreManager::find_node_index(const std::string &node_id) const {
	for (size_t i = 0; i < node_table.size(); ++i) {
		if (node_table[i].node_id == node_id) {
			return static_cast<int>(i);
		}
	}
	return -1;
}

// Query all keys from a storage node
std::vector<std::string> GTStoreManager::get_all_keys_from_node(const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for GET_ALL_KEYS");
		return {};
	}
	if (!send_message(fd, MessageType::GET_ALL_KEYS, "")) {
		log_line("WARN", "Failed to send GET_ALL_KEYS");
		close(fd);
		return {};
	}
	MessageType type;
	std::string payload;
	if (!recv_message(fd, type, payload)) {
		log_line("WARN", "Failed to receive ALL_KEYS response");
		close(fd);
		return {};
	}
	close(fd);
	if (type != MessageType::ALL_KEYS) {
		log_line("WARN", "Unexpected message type in GET_ALL_KEYS response");
		return {};
	}
	// Parse comma-separated keys
	std::vector<std::string> keys;
	if (payload.empty()) {
		return keys;
	}
	std::istringstream iss(payload);
	std::string key;
	while (std::getline(iss, key, ',')) {
		if (!key.empty()) {
			keys.push_back(key);
		}
	}
	return keys;
}

// Replicate a key-value pair to a specific node
void GTStoreManager::replicate_key_to_node(const std::string &key, const std::string &value, const NodeAddress &dest_addr) {
	int fd = connect_to_host(dest_addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for replication");
		return;
	}
	std::string payload = key + "|" + value;
	// Use REPL_PUT for rebalancing (not CLIENT_PUT) since manager won't send REPL_CONFIRM
	if (!send_message(fd, MessageType::REPL_PUT, payload)) {
		log_line("WARN", "Failed to send REPL_PUT to node");
		close(fd);
		return;
	}
	MessageType type;
	std::string response;
	if (!recv_message(fd, type, response)) {
		log_line("WARN", "Failed to receive PUT_OK");
		close(fd);
		return;
	}
	if (type != MessageType::PUT_OK) {
		log_line("WARN", "Unexpected response type, expected PUT_OK");
		close(fd);
		return;
	}
	close(fd);
}

// Get a single key-value pair from a specific node
std::pair<bool, std::string> GTStoreManager::get_key_from_node(const std::string &key, const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for GET");
		return {false, ""};
	}
	if (!send_message(fd, MessageType::CLIENT_GET, key)) {
		log_line("WARN", "Failed to send CLIENT_GET");
		close(fd);
		return {false, ""};
	}
	MessageType type;
	std::string payload;
	if (!recv_message(fd, type, payload)) {
		log_line("WARN", "Failed to receive GET response");
		close(fd);
		return {false, ""};
	}
	close(fd);
	if (type != MessageType::GET_OK) {
		log_line("WARN", "GET failed or key not found");
		return {false, ""};
	}
	return {true, payload};
}

// Delete a key from a specific node
void GTStoreManager::delete_key_from_node(const std::string &key, const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for deletion");
		return;
	}
	if (!send_message(fd, MessageType::CLIENT_DELETE, key)) {
		log_line("WARN", "Failed to send CLIENT_DELETE");
		close(fd);
		return;
	}
	MessageType type;
	std::string response;
	if (!recv_message(fd, type, response)) {
		log_line("WARN", "Failed to receive delete response");
		close(fd);
		return;
	}
	if (type != MessageType::DELETE_OK) {
		log_line("WARN", "Unexpected response type, expected DELETE_OK");
	}
	close(fd);
}

// Orchestrate rebalancing after a node failure
void GTStoreManager::rebalance_on_node_failure(int failed_idx, uint64_t failed_token, const std::vector<StorageNodeInfo> &nodes) {
	// nodes parameter is the post-failure snapshot (node already removed)
	if (nodes.empty()) {
		log_line("WARN", "No nodes remaining after failure, aborting rebalance");
		return;
	}
	
	log_line("INFO", "Starting rebalancing for failed node at idx=" + std::to_string(failed_idx) + " token=" + std::to_string(failed_token));
	
	// Find immediate predecessor and successor on the ring
	// Since the failed node is already removed, the successor is now at failed_idx (wrapping if needed)
	int predecessor_idx = (failed_idx - 1 + (int)nodes.size()) % (int)nodes.size();
	int successor_idx = failed_idx % (int)nodes.size(); // Node that was after failed is now at failed_idx
	
	log_line("INFO", "Querying immediate predecessor idx=" + std::to_string(predecessor_idx) + " and successor idx=" + std::to_string(successor_idx));
	
	// Query keys from both immediate neighbors
	std::vector<std::string> pred_keys = get_all_keys_from_node(nodes[predecessor_idx].address);
	std::vector<std::string> succ_keys = get_all_keys_from_node(nodes[successor_idx].address);
	log_line("INFO", "Predecessor has " + std::to_string(pred_keys.size()) + " keys, Successor has " + std::to_string(succ_keys.size()) + " keys");
	
	// Merge both key lists
	pred_keys.insert(pred_keys.end(), succ_keys.begin(), succ_keys.end());
	
	// For each key from both neighbors: rebalance if it's in the range that lost a replica
	std::hash<std::string> hasher;
	for (const auto &key : pred_keys) {
		uint64_t key_hash = hasher(key);
		
		// Only rebalance keys whose hash is less than the failed node's position
		// (i.e., keys that would have had the failed node as a replica)
		if (key_hash > failed_token) {
			continue; // This key wasn't affected by this node's failure
		}
		
		// Find primary node for this key (first node with token >= key_hash)
		int primary_idx = -1;
		for (size_t j = 0; j < nodes.size(); ++j) {
			if (nodes[j].token >= key_hash) {
				primary_idx = (int)j;
				break;
			}
		}
		if (primary_idx < 0) {
			primary_idx = 0; // Wrap around
		}
		
		// The K-th replica position for this key (which was on the failed node)
		int kth_replica_idx = (primary_idx + (int)(replication_factor - 1)) % (int)nodes.size();
		
		log_line("INFO", "Rebalancing key '" + key + "' to node idx " + std::to_string(kth_replica_idx));
		replicate_key_to_node(key, "rebalanced_value", nodes[kth_replica_idx].address);
	}
	
	log_line("INFO", "Rebalancing complete for failed node at idx=" + std::to_string(failed_idx));
}

// Orchestrate rebalancing when a new node joins
void GTStoreManager::rebalance_on_node_join(int new_idx, uint64_t new_token) {
	std::vector<StorageNodeInfo> nodes = snapshot_nodes();
	if (nodes.empty() || new_idx < 0 || new_idx >= (int)nodes.size()) {
		return;
	}
	
	log_line("INFO", "Starting rebalancing for new node at idx=" + std::to_string(new_idx) + " token=" + std::to_string(new_token));
	
	// Query keys from the immediate next node (it has all keys we might need to steal)
	int next_idx = (new_idx + 1) % (int)nodes.size();
	std::vector<std::string> next_node_keys = get_all_keys_from_node(nodes[next_idx].address);
	
	log_line("INFO", "Querying next node idx=" + std::to_string(next_idx) + ", found " + std::to_string(next_node_keys.size()) + " keys");
	
	// Process each key from the next node
	std::hash<std::string> hasher;
	for (const auto &key : next_node_keys) {
		uint64_t key_hash = hasher(key);
		
		// Only steal keys that belong to the new node's range (hash <= new_token)
		if (key_hash > new_token) {
			continue;
		}
		
		// Find the primary node for this key
		int primary_idx = -1;
		for (size_t j = 0; j < nodes.size(); ++j) {
			if (nodes[j].token >= key_hash) {
				primary_idx = (int)j;
				break;
			}
		}
		if (primary_idx < 0) {
			primary_idx = 0; // Wrap around
		}
		
		// Get the key-value pair from the primary node
		std::pair<bool, std::string> result = get_key_from_node(key, nodes[primary_idx].address);
		if (!result.first) {
			log_line("WARN", "Failed to get key '" + key + "' from primary node idx=" + std::to_string(primary_idx));
			continue;
		}
		std::string value = result.second;
		
		// Put the key on the new node
		log_line("INFO", "Stealing key '" + key + "' to new node at idx " + std::to_string(new_idx));
		replicate_key_to_node(key, value, nodes[new_idx].address);
		
		// Delete the key from the old K-th replica position (primary_idx + K)
		int old_kth_replica_idx = (primary_idx + (int)replication_factor) % (int)nodes.size();
		log_line("INFO", "Deleting key '" + key + "' from old K-th replica at idx " + std::to_string(old_kth_replica_idx));
		delete_key_from_node(key, nodes[old_kth_replica_idx].address);
	}
	
	log_line("INFO", "Rebalancing complete for new node at idx=" + std::to_string(new_idx));
}

void GTStoreManager::monitor_heartbeats() {
	const auto timeout = std::chrono::seconds(6);
	while (running) {
		std::this_thread::sleep_for(std::chrono::seconds(2));
		auto now = std::chrono::steady_clock::now();
		std::vector<std::pair<std::string, long long>> removed;
		{
			std::lock_guard<std::mutex> guard(table_mutex);
			auto it = node_table.begin();
			while (it != node_table.end()) {
				auto hb = heartbeat_times.find(it->node_id);
				bool expired = false;
				if (hb != heartbeat_times.end()) {
					if (now - hb->second > timeout) {
						expired = true;
					}
				} else {
					expired = true;
				}
				if (expired) {
					long long seconds_since = -1;
					if (hb != heartbeat_times.end()) {
						seconds_since = std::chrono::duration_cast<std::chrono::seconds>(now - hb->second).count();
					}
					int dead_idx = static_cast<int>(std::distance(node_table.begin(), it));
					uint64_t dead_token = it->token;
					std::string dead_node_id = it->node_id;
					removed.emplace_back(dead_node_id, seconds_since);
					heartbeat_times.erase(it->node_id);
					it = node_table.erase(it);
					log_line("WARN", "Node expired: " + dead_node_id + ", triggering rebalancing");
					// Call rebalancing while still holding the lock, passing the updated node table
					rebalance_on_node_failure(dead_idx, dead_token, node_table);
				} else {
					++it;
				}
			}
		}
		for (const auto &entry : removed) {
			std::string msg = "Removed dead storage " + entry.first;
			if (entry.second >= 0) {
				msg += " after " + std::to_string(entry.second) + "s without heartbeat";
			}
			log_line("WARN", msg);
		}
		if (!removed.empty()) {
			log_line("INFO", "Routing table snapshot: " + describe_nodes(snapshot_nodes()));
		}
	}
}

int main(int argc, char **argv) {

	GTStoreManager manager;
	manager.init();
	
}
