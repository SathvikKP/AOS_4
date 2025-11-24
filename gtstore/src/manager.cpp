#include "gtstore.hpp"
#include "utils.hpp"

#include <algorithm>
#include <functional>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <unordered_set>

using namespace gtstore_utils;
using namespace std;

#define BACKLOG 16

// This starts the manager listener.
void GTStoreManager::init() {
	setup_logging("manager");
	log_line("INFO", "Inside GTStoreManager::init()");
	char *env;

	uint16_t manager_port = DEFAULT_MANAGER_PORT;
	env = getenv("GTSTORE_MANAGER_PORT");
	if (env) {
		int parsed = atoi(env);
		if (parsed >= 0 && parsed <= 65535) {
			manager_port = static_cast<uint16_t>(parsed);
		}
	}

	string manager_address = DEFAULT_MANAGER_HOST;
	env = getenv("GTSTORE_MANAGER_HOST");
	if (env) {
		manager_address = string(env);
	}
	addr = NodeAddress{manager_address, manager_port};
	
	replication_factor = 1;
	running = true;
	env = getenv("GTSTORE_REPL");
	if (env) {
		int parsed = atoi(env);
		if (parsed >= 1) {
			replication_factor = static_cast<size_t>(parsed);
		}
	}

	log_line("INFO", "Replication factor set to " + to_string(replication_factor));
	listen_fd = create_listen_socket(addr, BACKLOG);
	if (listen_fd < 0) {
		log_line("ERROR", "Failed to create manager listen socket");
		return;
	}
	log_line("INFO", "Manager listening on " + addr.host + ":" + to_string(addr.port) + " with replication factor " + to_string(replication_factor));
	heartbeat_thread = thread(&GTStoreManager::monitor_heartbeats, this); // new thread for monitoring nodes for liveness
	heartbeat_thread.detach();
	accept_loop();
}

bool GTStoreManager::send_table(int client_fd, const vector<StorageNodeInfo> &nodes, size_t replication_factor) {
	log_line("INFO", "Sending routing table (rep=" + to_string(replication_factor) + "): " + describe_table(nodes));
	string payload = build_table_payload(nodes, replication_factor);
	return send_message(client_fd, MessageType::TABLE_PUSH, payload);
}

// This accepts client and storage sockets.
void GTStoreManager::accept_loop() {
	while (true) {
		int client_fd = accept_client(listen_fd);
		if (client_fd < 0) {
			continue;
		}
		thread([this, client_fd]() { // thread to handle each client
			MessageType type;
			string payload;
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
void GTStoreManager::handle_storage_register(const string &payload) {
	auto parts = gtstore_utils::split(payload, ',');
	if (parts.size() != 3) {
		log_line("WARN", "Invalid storage registration payload");
		return;
	}
	StorageNodeInfo info;
	info.node_id = parts[0];
	info.address.host = parts[1];
	info.address.port = static_cast<uint16_t>(stoi(parts[2]));
	hash<string> hasher;
	string token_seed = info.node_id + "-" + info.address.host + ":" + to_string(info.address.port);
	size_t idx = -1;
	info.token = static_cast<uint64_t>(hasher(token_seed));
	{
		lock_guard<mutex> guard(node_table_mutex);
		auto existing = find_if(node_table.begin(), node_table.end(), [&](const StorageNodeInfo &node) {
			return node.node_id == info.node_id;
		});
		if (existing != node_table.end()) {
			*existing = info;
			idx = distance(node_table.begin(), existing);
		} else {
			node_table.push_back(info);
			idx = node_table.size() - 1;
		}

		{
			lock_guard<mutex> hb_guard(heartbeat_mutex);
			heartbeat_times[info.node_id] = chrono::steady_clock::now(); // TODO: what if same node.id?
		}

		sort(node_table.begin(), node_table.end(), [](const StorageNodeInfo &lhs, const StorageNodeInfo &rhs) {
			return lhs.token < rhs.token;
		});
	}
	log_line("INFO", "Registered storage " + info.node_id + " at " + info.address.host + ":" + to_string(info.address.port));
	log_line("INFO", "Routing table snapshot: " + describe_table(snapshot_nodes()));
	
	// Trigger rebalancing for the new node
	if (idx >= 0 && node_table.size() > 1) {
		// Only rebalance if there are other nodes
		rebalance_on_node_join(idx, info.token); // TODO: block operations while rebalancing?
	}
}

// This records heartbeat timestamps.
void GTStoreManager::handle_heartbeat(const string &payload) {
	lock_guard<mutex> hb_guard(heartbeat_mutex);
	heartbeat_times[payload] = chrono::steady_clock::now();
}

// This copies current node table.
vector<StorageNodeInfo> GTStoreManager::snapshot_nodes() {
	lock_guard<mutex> guard(node_table_mutex);
	return node_table;
}

// This retrieves all keys from a storage node.
vector<string> GTStoreManager::get_all_keys_from_node(const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	vector<string> keys = {};
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for GET_ALL_KEYS");
		return keys;
	}
	if (!send_message(fd, MessageType::GET_ALL_KEYS, "")) {
		log_line("WARN", "Failed to send GET_ALL_KEYS");
		close(fd);
		return keys;
	}
	MessageType type;
	string payload;
	if (!recv_message(fd, type, payload) || type != MessageType::ALL_KEYS) {
		log_line("WARN", "Failed to receive ALL_KEYS response");
		close(fd);
		return keys;
	}
	close(fd);

	keys = split(payload, ',');

	return keys;
}

// This replicates key-value pairs to a specific node.
void GTStoreManager::replicate_key_to_node(const vector<string> &keys, const vector<string> &values, const NodeAddress &dest_addr) {
	int fd = connect_to_host(dest_addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for replication");
		return;
	}

	vector<string> kv_pairs;
	for (size_t i = 0; i < keys.size(); ++i) {
		kv_pairs.push_back(keys[i] + "|" + values[i]);
	}
	string payload = join(kv_pairs, ';');

	// Use REPL_PUT for rebalancing (not CLIENT_PUT) since manager won't send REPL_CONFIRM
	if (!send_message(fd, MessageType::REPL_PUT, payload)) {
		log_line("WARN", "Failed to send REPL_PUT to node");
		close(fd);
		return;
	}
	MessageType type;
	string response;
	if (!recv_message(fd, type, response) || type != MessageType::PUT_OK) {
		log_line("WARN", "Failed to receive PUT_OK");
		close(fd);
		return;
	}

	close(fd);
}

// This gets key-value pairs from a specific node.
vector<string> GTStoreManager::get_key_from_node(const vector<string> &keys, const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	vector<string> values;
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for GET");
		return values;
	}
	
	string keys_payload = join(keys, ';');
	
	if (!send_message(fd, MessageType::CLIENT_GET, keys_payload)) {
		log_line("WARN", "Failed to send CLIENT_GET");
		close(fd);
		return values;
	}
	
	MessageType type;
	string values_payload;
	if (!recv_message(fd, type, values_payload) || type != MessageType::GET_OK) {
		log_line("WARN", "Failed to receive GET response");
		close(fd);
		return values;
	}
	close(fd);


	values = split(values_payload, ';');
	return values;
}

// This deletes keys from a specific node.
void GTStoreManager::delete_key_from_node(const vector<string> &keys, const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for deletion");
		return;
	}

	string keys_payload = join(keys, ";");

	if (!send_message(fd, MessageType::CLIENT_DELETE, keys_payload)) {
		log_line("WARN", "Failed to send CLIENT_DELETE");
		close(fd);
		return;
	}
	MessageType type;
	string response;
	if (!recv_message(fd, type, response) || type != MessageType::DELETE_OK) {
		log_line("WARN", "Failed to receive delete response");
	}
	close(fd);
}

// This rebalances the keys after a node failure
void GTStoreManager::rebalance_on_node_failure(int failed_idx, uint64_t failed_token, const vector<StorageNodeInfo> &nodes) {
	// nodes parameter is the post-failure snapshot (node already removed)
	if (nodes.empty()) {
		log_line("WARN", "No nodes remaining after failure, aborting rebalance");
		return;
	}
	
	log_line("INFO", "Starting rebalancing for failed node at idx=" + to_string(failed_idx) + " token=" + to_string(failed_token));
	
	// find immediate predecessor and successor on the ring
	// since the failed node is already removed, the successor is now at failed_idx (wrapping if needed)
	int predecessor_idx = (failed_idx - 1 + (int)nodes.size()) % (int)nodes.size();
	int successor_idx = failed_idx % (int)nodes.size();
	int earliest_predecessor_idx = (failed_idx - (int)(replication_factor - 1) + (int)nodes.size()) % (int)nodes.size();

	log_line("INFO", "Querying immediate predecessor idx=" + to_string(predecessor_idx) + " and successor idx=" + to_string(successor_idx));
	
	vector<string> pred_keys = get_all_keys_from_node(nodes[predecessor_idx].address);
	vector<string> succ_keys = get_all_keys_from_node(nodes[successor_idx].address);
	log_line("INFO", "Predecessor has " + to_string(pred_keys.size()) + " keys, Successor has " + to_string(succ_keys.size()) + " keys");
	
	unordered_set<string> all_keys(pred_keys.begin(), pred_keys.end());
	all_keys.insert(succ_keys.begin(), succ_keys.end());
	
	// map to batch operations: [source_idx, dest_idx] = {keys}
	map<pair<int, int>, vector<string>> keys_to_move;
	
	// for each key from both neighbors: rebalance if it's in the range that lost a replica
	hash<string> hasher;
	for (const auto &key : all_keys) {
		uint64_t key_hash = hasher(key);
		
		int primary_idx = -1;
		for (size_t j = 0; j < nodes.size(); ++j) {
			if (nodes[j].token >= key_hash) {
				primary_idx = (int)j;
				break;
			}
		}
		if (primary_idx < 0) {
			primary_idx = 0; // wrap around
		}
		
		// primary node of key should be within [predecessor_idx, successor_idx]
		bool in_range = false;
		if (primary_idx == successor_idx) {
			in_range = (key_hash <= failed_token); // ignore next node's primary keys
		} else if (earliest_predecessor_idx <= successor_idx) {
			in_range = (primary_idx >= earliest_predecessor_idx && primary_idx < successor_idx);
		} else {
			// wrap around case
			in_range = (primary_idx >= earliest_predecessor_idx || primary_idx < successor_idx);
		}
		
		if (!in_range) {
			continue;
		}
		
		// the K-th replica position for this key (which was on the failed node)
		int kth_replica_idx = (primary_idx + (int)(replication_factor - 1)) % (int)nodes.size();
		
		keys_to_move[{primary_idx, kth_replica_idx}].push_back(key);
	}
	
	for (const auto &entry : keys_to_move) {
		int source_idx = entry.first.first;
		int dest_idx = entry.first.second;
		const vector<string> &keys = entry.second;
		
		log_line("INFO", "Moving " + to_string(keys.size()) + " keys from idx=" + to_string(source_idx) + " to idx=" + to_string(dest_idx));
		
		vector<string> values = get_key_from_node(keys, nodes[source_idx].address);
		if (values.size() != keys.size()) {
			log_line("WARN", "Failed to get all keys from source node");
			continue;
		}
		
		replicate_key_to_node(keys, values, nodes[dest_idx].address);
		
		log_line("INFO", "Successfully moved " + to_string(keys.size()) + " keys");
	}
	
	log_line("INFO", "Rebalancing complete for failed node at idx=" + to_string(failed_idx) + " token=" + to_string(failed_token));
}

// This rebalances the keys when a new node joins
void GTStoreManager::rebalance_on_node_join(int new_idx, uint64_t new_token) {
	vector<StorageNodeInfo> nodes = snapshot_nodes();
	
	log_line("INFO", "Starting rebalancing for new node at idx=" + to_string(new_idx) + " token=" + to_string(new_token));
	
	// get keys from the immediate next node (it has all keys we might need to steal)
	int next_idx = (new_idx + 1) % (int)nodes.size();
	vector<string> next_node_keys = get_all_keys_from_node(nodes[next_idx].address);
	
	log_line("INFO", "Querying next node idx=" + to_string(next_idx) + ", found " + to_string(next_node_keys.size()) + " keys");
	
	// map: [source_idx] = {keys to copy to new_idx}
	map<int, vector<string>> keys_to_copy;
	// map: [old_kth_replica_idx] = {keys to delete}
	map<int, vector<string>> keys_to_delete;
	
	hash<string> hasher;
	for (const auto &key : next_node_keys) {
		uint64_t key_hash = hasher(key);
		
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
		
		// only steal keys if the new node is a replica for this key
		// check if new_idx is in the replica range [primary_idx, primary_idx+K-1]
		bool is_replica = false;
		int last_replica_idx = (primary_idx + (int)(replication_factor - 1)) % (int)nodes.size();
		if (primary_idx <= last_replica_idx) {
			is_replica = (new_idx >= primary_idx && new_idx <= last_replica_idx);
		} else {
			// wrap around case
			is_replica = (new_idx >= primary_idx || new_idx <= last_replica_idx);
		}
		
		if (!is_replica) {
			continue;
		}
		
		keys_to_copy[primary_idx].push_back(key);
		
		// delete the key from the old K-th replica position (primary_idx + K)
		int old_kth_replica_idx = (last_replica_idx + 1) % (int)nodes.size();
		keys_to_delete[old_kth_replica_idx].push_back(key);
	}
	
	for (const auto &entry : keys_to_copy) {
		int source_idx = entry.first;
		const vector<string> &keys = entry.second;
		
		if (keys.empty()) continue;
		
		log_line("INFO", "Moving " + to_string(keys.size()) + " keys from idx=" + to_string(source_idx) + " to new node idx=" + to_string(new_idx));
		
		vector<string> values = get_key_from_node(keys, nodes[source_idx].address);
		if (values.size() != keys.size()) {
			log_line("WARN", "Failed to get all keys from source node");
			continue;
		}
		
		replicate_key_to_node(keys, values, nodes[new_idx].address);
		log_line("INFO", "Successfully moved " + to_string(keys.size()) + " keys");
	}
	
	for (const auto &entry : keys_to_delete) {
		int old_kth_replica_idx = entry.first;
		const vector<string> &keys = entry.second;
		
		if (keys.empty()) continue;

		log_line("INFO", "Deleting " + to_string(keys.size()) + " keys from old K-th replica idx=" + to_string(old_kth_replica_idx));
		delete_key_from_node(keys, nodes[old_kth_replica_idx].address);
	}
	
	log_line("INFO", "Rebalancing complete for new node at idx=" + to_string(new_idx) + " token=" + to_string(new_token));
}

void GTStoreManager::monitor_heartbeats() {
	const auto timeout = chrono::seconds(6);
	while (running) {
		this_thread::sleep_for(chrono::seconds(2));
		auto now = chrono::steady_clock::now();
		vector<pair<string, long long>> removed;
		{
			lock_guard<mutex> guard(node_table_mutex);
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
						seconds_since = chrono::duration_cast<chrono::seconds>(now - hb->second).count();
					}
					int dead_idx = static_cast<int>(distance(node_table.begin(), it));
					uint64_t dead_token = it->token;
					string dead_node_id = it->node_id;
					removed.emplace_back(dead_node_id, seconds_since);
					{
						lock_guard<mutex> hb_guard(heartbeat_mutex);
						heartbeat_times.erase(it->node_id);
					}
					it = node_table.erase(it);
					log_line("WARN", "Node expired: " + dead_node_id + ", triggering rebalancing");
					// call rebalancing while still holding the lock, passing the updated node table
					rebalance_on_node_failure(dead_idx, dead_token, node_table);
				} else {
					++it;
				}
			}
		}
		for (const auto &entry : removed) {
			string msg = "Removed dead storage " + entry.first;
			if (entry.second >= 0) {
				msg += " after " + to_string(entry.second) + "s without heartbeat";
			}
			log_line("WARN", msg);
		}
		if (!removed.empty()) {
			log_line("INFO", "Routing table snapshot: " + describe_table(snapshot_nodes()));
		}
	}
}

int main(int argc, char **argv) {

	GTStoreManager manager;
	manager.init();
	
}
