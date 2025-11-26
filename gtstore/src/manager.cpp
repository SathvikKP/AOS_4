#include "gtstore.hpp"
#include "utils.hpp"

#include <algorithm>
#include <functional>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <unordered_set>
#include <map>
#include <set>

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
					case MessageType::STORAGE_REGISTER: {
						string registered_node_id = handle_storage_register(payload);
						send_table(client_fd, snapshot_nodes(), replication_factor);
						close(client_fd);

						rebalance_on_node_join(registered_node_id);
						broadcast_table_to_storage_nodes();
						break;
					}
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
string GTStoreManager::handle_storage_register(const string &payload) {
	auto parts = gtstore_utils::split(payload, ',');
	if (parts.size() != 3) {
		log_line("WARN", "Invalid storage registration payload");
		return {};
	}
	string node_id = parts[0];
	NodeAddress address;
	address.host = parts[1];
	address.port = static_cast<uint16_t>(stoi(parts[2]));
	
	vector<uint64_t> virtual_tokens = generate_virtual_tokens(node_id, VNODES_PER_NODE);
	
	{
		lock_guard<mutex> guard(node_table_mutex);
		
		// remove all old virtual nodes for this physical node
		auto it = node_table.begin();
		while (it != node_table.end()) {
			if (it->node_id == node_id) {
				it = node_table.erase(it);
			} else {
				++it;
			}
		}

		for (int i = 0; i < VNODES_PER_NODE; ++i) {
			StorageNodeInfo vnode;
			vnode.node_id = node_id;
			vnode.address = address;
			vnode.token = virtual_tokens[i];
			node_table.push_back(vnode);
		}
		{
			lock_guard<mutex> hb_guard(heartbeat_mutex);
			heartbeat_times[node_id] = chrono::steady_clock::now();
		}

		sort(node_table.begin(), node_table.end(), [](const StorageNodeInfo &lhs, const StorageNodeInfo &rhs) {
			return lhs.token < rhs.token;
		});
	}
	
	log_line("INFO", "Registered storage " + node_id + " at " + address.host + ":" + to_string(address.port) + " with " + to_string(VNODES_PER_NODE) + " virtual nodes");
	log_line("INFO", "Routing table snapshot: " + describe_table(snapshot_nodes()));

	return node_id;
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

// This broadcasts the current routing table to all storage nodes.
void GTStoreManager::broadcast_table_to_storage_nodes() {
	auto nodes = snapshot_nodes();
	string payload = build_table_payload(nodes, replication_factor);
	
	unordered_set<string> broadcasted_nodes;
	log_line("INFO", "Broadcasting routing table to storage nodes");
	
	for (const auto &node : nodes) {
		if (broadcasted_nodes.find(node.node_id) != broadcasted_nodes.end()) {
			continue;
		}
		broadcasted_nodes.insert(node.node_id);
		
		int fd = connect_to_host(node.address);
		if (fd < 0) {
			log_line("WARN", "Failed to connect to " + node.node_id + " for table broadcast");
			continue;
		}
		
		if (!send_message(fd, MessageType::TABLE_PUSH, payload)) {
			log_line("WARN", "Failed to send table to " + node.node_id);
			close(fd);
			continue;
		}
		
		MessageType resp_type;
		string resp_payload;
		if (!recv_message(fd, resp_type, resp_payload)) {
			log_line("WARN", "No ack from " + node.node_id + " after table broadcast");
			close(fd);
			continue;
		}
		
		close(fd);
		log_line("INFO", "Table broadcast sent to " + node.node_id);
	}
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
	
	if (!send_message(fd, MessageType::MANAGER_GET, keys_payload)) {
		log_line("WARN", "Failed to send MANAGER_GET");
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

	string keys_payload = join(keys, ';');

	if (!send_message(fd, MessageType::MANAGER_DELETE, keys_payload)) {
		log_line("WARN", "Failed to send MANAGER_DELETE");
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

// This pauses a storage node (blocks client requests during rebalancing).
bool GTStoreManager::pause_node(const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for pause");
		return false;
	}
	
	if (!send_message(fd, MessageType::PAUSE_NODE, "")) {
		log_line("WARN", "Failed to send PAUSE_NODE");
		close(fd);
		return false;
	}
	
	MessageType type;
	string response;
	bool success = recv_message(fd, type, response) && type == MessageType::PAUSE_ACK;
	close(fd);
	
	if (success) {
		log_line("INFO", "Node paused successfully: " + addr.host + ":" + to_string(addr.port));
	} else {
		log_line("WARN", "Node did not ack pause");
	}
	return success;
}

// This resumes a storage node (allows client requests after rebalancing).
bool GTStoreManager::resume_node(const NodeAddress &addr) {
	int fd = connect_to_host(addr);
	if (fd < 0) {
		log_line("WARN", "Failed to connect to node for resume");
		return false;
	}
	
	if (!send_message(fd, MessageType::RESUME_NODE, "")) {
		log_line("WARN", "Failed to send RESUME_NODE");
		close(fd);
		return false;
	}
	
	MessageType type;
	string response;
	bool success = recv_message(fd, type, response) && type == MessageType::RESUME_ACK;
	close(fd);
	
	if (success) {
		log_line("INFO", "Node resumed successfully: " + addr.host + ":" + to_string(addr.port));
	} else {
		log_line("WARN", "Node did not ack resume");
	}
	return success;
}

// This waits for a node to become available (no ongoing PUTs with locks held).
bool GTStoreManager::wait_for_availability(const NodeAddress &addr, int max_attempts) {
	for (int attempt = 0; attempt < max_attempts; ++attempt) {
		int fd = connect_to_host(addr);
		if (fd < 0) {
			log_line("WARN", "Failed to connect to node for availability check");
			return false;
		}

		if (!send_message(fd, MessageType::AVAILABILITY_CHECK, "")) {
			log_line("WARN", "Failed to send AVAILABILITY_CHECK");
			close(fd);
			return false;
		}
		
		MessageType type;
		string response;
		if (!recv_message(fd, type, response) || type != MessageType::AVAILABLE_STATUS) {
			log_line("WARN", "Failed to receive AVAILABLE_STATUS");
			close(fd);
			return false;
		}
		close(fd);
		
		if (response == "yes") {
			log_line("INFO", "Node is available: " + addr.host + ":" + to_string(addr.port));
			return true;
		}
		
		// Node still has locks, wait and retry
		log_line("INFO", "Node not yet available (attempt " + to_string(attempt + 1) + "/" + to_string(max_attempts) + "), waiting...");
		this_thread::sleep_for(chrono::milliseconds(200));
	}

	log_line("WARN", "Node did not become available after " + to_string(max_attempts) + " attempts");
	return false;
}

// This rebalances the keys after a node failure
void GTStoreManager::rebalance_on_node_failure(const string &failed_physical_node_id, const vector<StorageNodeInfo> &nodes) {
	// nodes parameter is the pre-failure snapshot
	if (nodes.empty()) {
		log_line("WARN", "No nodes remaining after failure, aborting rebalance");
		return;
	}
	
	log_line("INFO", "Starting rebalancing for failed physical node: " + failed_physical_node_id);

	set<string> query_nodes;  // physical node IDs that are predecessors
	map<string, NodeAddress> node_to_address;
	map<int, pair<int, int>> neighbor_vnodes;
	// find immediate predecessor and successor for each failed vnode
	for (int cur_idx = 0; cur_idx < (int)nodes.size(); ++cur_idx) {
		if (nodes[cur_idx].node_id != failed_physical_node_id) {
			continue;
		}
		
		// immediate predecessor
		int predecessor_idx = -1;
		int idx = (cur_idx - 1 + (int)nodes.size()) % (int)nodes.size();
		while (idx != cur_idx) {
			if (nodes[idx].node_id != failed_physical_node_id) {
				predecessor_idx = idx;
				break;
			}
			idx = (idx - 1 + (int)nodes.size()) % (int)nodes.size();
		}

		// successor
		int successor_idx = -1;
		idx = (cur_idx + 1) % (int)nodes.size();
		while (idx != cur_idx) {
			if (nodes[idx].node_id != failed_physical_node_id) {
				successor_idx = idx;
				break;
			}
			idx = (idx + 1) % (int)nodes.size();
		}
		
		neighbor_vnodes[cur_idx] = {predecessor_idx, successor_idx};
		query_nodes.insert(nodes[predecessor_idx].node_id);
		query_nodes.insert(nodes[successor_idx].node_id);
		node_to_address[nodes[predecessor_idx].node_id] = nodes[predecessor_idx].address;
		node_to_address[nodes[successor_idx].node_id] = nodes[successor_idx].address;
	}

	// collect keys from the nodes
	unordered_set<string> all_keys;
	for (const string &physical_node_id: query_nodes) {
		vector<string> node_keys = get_all_keys_from_node(node_to_address[physical_node_id]);
		all_keys.insert(node_keys.begin(), node_keys.end()); 
	}
	
	// map to batch operations: [source_addr, dest_addr] = keys
	// for each key from both neighbors: rebalance if it's in the range that lost a replica
	map<pair<NodeAddress, NodeAddress>, vector<string>> keys_to_move;
	set<NodeAddress> affected_nodes;
	
	for (const auto &key : all_keys) {
		uint64_t key_hash = gtstore_utils::consistent_hash(key);

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

		// check if any of the k replicas were on the failed node
		int k = 0;
		int cur_idx = primary_idx;
		int upper_limit = replication_factor;
		int originator = (nodes[primary_idx].node_id != failed_physical_node_id) ? primary_idx : neighbor_vnodes[primary_idx].second;
		set<string> seen_physical_nodes;
		bool to_move = false;
		while (k < upper_limit && k < (int)nodes.size()){
			if (seen_physical_nodes.find(nodes[cur_idx].node_id) == seen_physical_nodes.end()){
				seen_physical_nodes.insert(nodes[cur_idx].node_id);
				if (nodes[cur_idx].node_id == failed_physical_node_id){
					to_move = true;
					upper_limit++; // need to check one more physical replica
				}
				k++;
			}
			cur_idx = (cur_idx + 1) % (int)nodes.size();
		}
		if (!to_move){
			continue;
		}

		cur_idx = (cur_idx - 1 + (int)nodes.size()) % (int)nodes.size(); // step back to last replica
		keys_to_move[{nodes[originator].address, nodes[cur_idx].address}].push_back(key);
	}

	for (const auto &entry : keys_to_move) {
		affected_nodes.insert(entry.first.first);   // source
		affected_nodes.insert(entry.first.second);  // destination
	}
		
	// pause all affected nodes before rebalancing
	log_line("INFO", "Pausing " + to_string(affected_nodes.size()) + " affected nodes for rebalancing");
	for (const auto &addr : affected_nodes) {
		pause_node(addr);
	}
	
	// wait for all affected nodes to become available
	log_line("INFO", "Waiting for nodes to become available");
	for (const auto &addr : affected_nodes) {
		if (!wait_for_availability(addr)) {
			log_line("WARN", "Node did not become available");
		}
	}
	log_line("INFO", "All nodes available, starting rebalancing operations");

	for (const auto &entry : keys_to_move) {
		NodeAddress source_addr = entry.first.first;
		NodeAddress dest_addr = entry.first.second;
		const vector<string> &keys = entry.second;

		log_line("INFO", "Moving " + to_string(keys.size()) + " keys from " + source_addr.host + ":" + to_string(source_addr.port) + " to " + dest_addr.host + ":" + to_string(dest_addr.port));

		vector<string> values = get_key_from_node(keys, source_addr);
		if (values.size() != keys.size()) {
			log_line("WARN", "Failed to get all keys from source node");
			continue;
		}

		replicate_key_to_node(keys, values, dest_addr);

		log_line("INFO", "Successfully moved " + to_string(keys.size()) + " keys");
	}
	
	// resume affected nodes after rebalancing
	log_line("INFO", "Resuming affected nodes after rebalancing");
	for (const auto &addr : affected_nodes) {
		resume_node(addr);
	}
	
	log_line("INFO", "Rebalancing complete for failed physical node: " + failed_physical_node_id);
}

// This rebalances the keys when a new node joins
void GTStoreManager::rebalance_on_node_join(const string &new_node_id) {
	vector<StorageNodeInfo> nodes = snapshot_nodes();
	map<int, int> neighbor_vnodes;
	// find all the vnodes for the new physical node
	set<NodeAddress> successor_indices;
	set<uint64_t> queue_indices;
	set<NodeAddress> affected_nodes;
	NodeAddress new_node_address;
	bool to_find = false;
	for (size_t i = 0; i < nodes.size(); ++i) {
		if (nodes[i].node_id == new_node_id) {
			new_node_address = nodes[i].address;
			queue_indices.insert(i);
			to_find = true;
		}
		if (to_find && nodes[i].node_id != new_node_id) {
			successor_indices.insert(nodes[i].address);
			for (uint64_t idx : queue_indices) {
				neighbor_vnodes[idx] = i;
			}
			queue_indices.clear();
			to_find = false;
		}
	}
	if (to_find){
		// wrap around case
		for (uint64_t idx=0; idx < nodes.size(); ++idx){
			if (nodes[idx].node_id != new_node_id){
				successor_indices.insert(nodes[idx].address);
				for (uint64_t q_idx : queue_indices) {
					neighbor_vnodes[q_idx] = idx;
				}
				break;
			}
		}
	}

	log_line("INFO", "Starting rebalancing for new node with tokens: " + to_string(neighbor_vnodes.size()));
	
	// get keys from the immediate next nodes (it has all keys we might need to steal)
	set<string> next_node_keys;

	affected_nodes.insert(new_node_address);
	for (const auto &addr : successor_indices) {
		vector<string> keys = get_all_keys_from_node(addr);
		next_node_keys.insert(keys.begin(), keys.end());
	}

	log_line("INFO", "Querying next nodes " + to_string(successor_indices.size()) + ", found " + to_string(next_node_keys.size()) + " keys");

	// map: [source_idx] = {keys to copy to new_idx}
	map<NodeAddress, vector<string>> keys_to_copy;
	// map: [old_kth_replica_idx] = {keys to delete}
	map<NodeAddress, vector<string>> keys_to_delete;
		
	for (const auto &key : next_node_keys) {
		uint64_t key_hash = gtstore_utils::consistent_hash(key);
		
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

		// check if any of the k replicas are supposed to be on the new node
		// if so, also find the k+1 to be removed
		int k = 0;
		int cur_idx = primary_idx;
		int upper_limit = replication_factor;
		int originator = (nodes[primary_idx].node_id != new_node_id) ? primary_idx : neighbor_vnodes[primary_idx];
		set<string> seen_physical_nodes;
		bool to_move = false;
		while (k < upper_limit && k < (int)nodes.size()){
			if (seen_physical_nodes.find(nodes[cur_idx].node_id) == seen_physical_nodes.end()){
				seen_physical_nodes.insert(nodes[cur_idx].node_id);
				if (nodes[cur_idx].node_id == new_node_id){
					to_move = true;
					upper_limit++; // need to check one more physical replica
				}
				k++;
			}
			cur_idx = (cur_idx + 1) % (int)nodes.size();
		}
		if (!to_move){
			continue;
		}

		keys_to_copy[nodes[originator].address].push_back(key);
		affected_nodes.insert(nodes[originator].address);
		
		// delete the key from the old K-th replica position (primary_idx + K)
		cur_idx = (cur_idx - 1 + (int)nodes.size()) % (int)nodes.size(); // step back to last replica
		keys_to_delete[nodes[cur_idx].address].push_back(key);
		affected_nodes.insert(nodes[cur_idx].address);
	}
	
	// pause all affected nodes before rebalancing
	log_line("INFO", "Pausing " + to_string(affected_nodes.size()) + " affected nodes for rebalancing");
	for (const auto &addr : affected_nodes) {
		pause_node(addr);
	}
	
	// wait for all affected nodes to become available (no ongoing PUTs)
	log_line("INFO", "Waiting for nodes to become available");
	for (const auto &addr : affected_nodes) {
		if (!wait_for_availability(addr)) {
			log_line("WARN", "Node " + addr.host + ":" + to_string(addr.port) + " did not become available");
		}
	}
	log_line("INFO", "All nodes available, starting rebalancing operations");

	for (const auto &entry : keys_to_copy) {
		const vector<string> &keys = entry.second;
		
		if (keys.empty()) continue;

		log_line("INFO", "Moving " + to_string(keys.size()) + " keys from addr=" + entry.first.host + ":" + to_string(entry.first.port) + " to new node addr=" + new_node_address.host + ":" + to_string(new_node_address.port));

		vector<string> values = get_key_from_node(keys, entry.first);
		if (values.size() != keys.size()) {
			log_line("WARN", "Failed to get all keys from source node");
			continue;
		}

		replicate_key_to_node(keys, values, new_node_address);
		log_line("INFO", "Successfully moved " + to_string(keys.size()) + " keys");
	}
	
	for (const auto &entry : keys_to_delete) {
		const vector<string> &keys = entry.second;
		
		if (keys.empty()) continue;

		log_line("INFO", "Deleting " + to_string(keys.size()) + " keys from old K-th replica addr=" + entry.first.host + ":" + to_string(entry.first.port));
		delete_key_from_node(keys, entry.first);
	}
	
	// resume affected nodes after rebalancing
	log_line("INFO", "Resuming affected nodes after rebalancing");
	for (const auto &addr : affected_nodes) {
		resume_node(addr);
	}
	
	log_line("INFO", "Rebalancing complete for new node with ID: " + new_node_id);
}

void GTStoreManager::monitor_heartbeats() {
	const auto timeout = chrono::seconds(6);
	while (running) {
		this_thread::sleep_for(chrono::seconds(2));
		auto now = chrono::steady_clock::now();
		
		set<string> expired_physical_nodes;
		vector<pair<string, long long>> removed;
		vector<StorageNodeInfo> node_table_copy;
		
		{
			lock_guard<mutex> guard(node_table_mutex);
			node_table_copy = node_table;
		}
		
		for (const auto &node : node_table_copy) {
			auto hb = heartbeat_times.find(node.node_id);
			bool expired = false;
			
			if (hb != heartbeat_times.end()) {
				if (now - hb->second > timeout) {
					expired = true;
				}
			} else {
				expired = true;
			}
			
			if (expired) {
				if (expired_physical_nodes.find(node.node_id) == expired_physical_nodes.end()) {
					expired_physical_nodes.insert(node.node_id);
					
					long long seconds_since = -1;
					if (hb != heartbeat_times.end()) {
						seconds_since = chrono::duration_cast<chrono::seconds>(now - hb->second).count();
					}
					removed.emplace_back(node.node_id, seconds_since);
					
					log_line("WARN", "Node expired: " + node.node_id + ", triggering rebalancing");
				}
			}
		}
		
		if (!expired_physical_nodes.empty()) {
			{
				lock_guard<mutex> guard(node_table_mutex);
				for (const auto &expired_node_id : expired_physical_nodes) {
					rebalance_on_node_failure(expired_node_id, node_table);
				}
				
				auto it = node_table.begin();
				while (it != node_table.end()) {
					if (expired_physical_nodes.find(it->node_id) != expired_physical_nodes.end()) {
						it = node_table.erase(it);
					} else {
						++it;
					}
				}
			}
			{
				lock_guard<mutex> hb_guard(heartbeat_mutex);
				for (const auto &expired_node_id : expired_physical_nodes) {
					heartbeat_times.erase(expired_node_id);
				}
			}
			
			for (const auto &entry : removed) {
				string msg = "Removed dead storage " + entry.first;
				if (entry.second >= 0) {
					msg += " after " + to_string(entry.second) + "s without heartbeat";
				}
				log_line("WARN", msg);
			}
			
			log_line("INFO", "Routing table snapshot: " + describe_table(snapshot_nodes()));
			broadcast_table_to_storage_nodes();
		}
	}
}

int main(int argc, char **argv) {

	GTStoreManager manager;
	manager.init();
	
}
