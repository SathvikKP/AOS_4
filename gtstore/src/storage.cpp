#include "gtstore.hpp"
#include "utils.hpp"

#include <cstdlib>
#include <sstream>
#include <thread>

using namespace gtstore_utils;
using namespace std;

namespace {
const string COMPONENT_PREFIX = "storage_";
const int BACKLOG = 16;
}

// This tells manager about this storage node.
bool GTStoreStorage::register_with_manager() {
	int fd = connect_to_host(manager_addr);
	if (fd < 0) {
		log_line("ERROR", "could not reach manager");
		return false;
	}
	string payload = storage_id + "," + addr.host + "," + to_string(addr.port); // remember to mention fixed address to report
	if (!send_message(fd, MessageType::STORAGE_REGISTER, payload)) {
		log_line("ERROR", "failed to send register");
		close(fd);
		return false;
	}
	MessageType type;
	string table_payload;
	if (recv_message(fd, type, table_payload) && type == MessageType::TABLE_PUSH) {
		size_t parsed_factor = 1;
		auto nodes = parse_table_payload(table_payload, parsed_factor);
		replication_factor = parsed_factor;
		routing_table = nodes;
		log_line("INFO", "Received table with " + to_string(nodes.size()) + " nodes at replication " + to_string(replication_factor));
		close(fd);
		return true;
	}
	close(fd);
	return false;
}

// This sends heartbeat messages to manager.
void GTStoreStorage::heartbeat_loop() {
	while (running) {
		this_thread::sleep_for(chrono::seconds(2));
		int fd = connect_to_host(manager_addr);
		if (fd < 0) {
			continue;
		}
		if (!send_message(fd, MessageType::HEARTBEAT, storage_id)) {
			close(fd);
			continue;
		}
		MessageType type;
		string payload;
		recv_message(fd, type, payload); // TODO: remove HEARTBEAT_ACK handling?
		close(fd);
	}
}

// This checks the key size.
bool GTStoreStorage::key_valid(const string &key) {
	return !key.empty() && key.size() <= MAX_KEY_BYTE_PER_REQUEST;
}

// This checks the value size.
bool GTStoreStorage::value_valid(const string &value) {
	return value.size() <= MAX_VALUE_BYTE_PER_REQUEST;
}

// This stores keys locally.
void GTStoreStorage::handle_put(int client_fd, const string &payload, bool is_primary) {
	vector<string> pairs = split(payload, ';');
	if (pairs.empty()) {
		send_message(client_fd, MessageType::ERROR, "no pairs");
		return;
	}
	
	vector<pair<string, string>> kv_pairs;
	for (const auto &kv_pair : pairs) {
		auto pos = kv_pair.find('|');
		if (pos == string::npos) {
			send_message(client_fd, MessageType::ERROR, "bad put format: " + kv_pair);
			return;
		}
		string key = kv_pair.substr(0, pos);
		string value = kv_pair.substr(pos + 1);

		if (!key_valid(key)) {
			send_message(client_fd, MessageType::ERROR, "bad key: " + key);
			return;
		}
		if (!value_valid(value)) {
			send_message(client_fd, MessageType::ERROR, "bad value for key: " + key);
			return;
		}
		kv_pairs.push_back({key, value});
	}
	
	// Only primary needs to acquire locks
	string client_id = "client_" + to_string(client_fd);
	vector<string> acquired_locks;
	if (is_primary) {
		for (const auto &kv_pair : kv_pairs) {
			if (!try_acquire_lock(kv_pair.first, client_id)) {
				send_message(client_fd, MessageType::ERROR, "locked: " + kv_pair.first);
				log_line("WARN", "PUT rejected key=" + kv_pair.first + " (locked) on " + storage_id);
				return;
			}
			acquired_locks.push_back(kv_pair.first);
		}
	}

	for (const auto &kv_pair : kv_pairs) {
		log_line("INFO", "PUT key=" + kv_pair.first + " value=" + kv_pair.second + " on " + storage_id);
		kv_store[kv_pair.first] = kv_pair.second;
	}
	log_current_store();
	
	// If primary, forward to all other K-1 replicas for this key (excluding self)
	if (is_primary) {
		int cur_idx = -1;
		for (size_t i = 0; i < routing_table.size(); ++i) {
			if (routing_table[i].node_id == storage_id) {
				cur_idx = static_cast<int>(i);
				break;
			}
		}

		if (cur_idx == -1) {
			log_line("ERROR", "Cannot find self storage in routing table");
			send_message(client_fd, MessageType::ERROR, "routing error");
			for (const auto &key : acquired_locks) {
				release_lock(key);
			}
			return;
		}
		
		// get primary index for first key to determine the k replicas
		string first_key = kv_pairs[0].first;
		uint64_t key_hash = hash<string>{}(first_key);
		
		int primary_idx = 0;
		for (size_t i = 0; i < routing_table.size(); ++i) {
			if (key_hash <= routing_table[i].token) {
				primary_idx = static_cast<int>(i);
				break;
			}
		}
		
		size_t successful_replicas = 1;
		for (size_t rep = 0; rep < replication_factor; ++rep) {
			int replica_idx = (primary_idx + static_cast<int>(rep)) % static_cast<int>(routing_table.size());
			if (replica_idx == cur_idx) {
				continue; // skip self
			}
			
			const auto &replica_info = routing_table[replica_idx];
			
			int replica_fd = connect_to_host(replica_info.address);
			if (replica_fd < 0) {
				log_line("WARN", "Failed to connect to replica " + replica_info.node_id);
				continue; // try next
			}
			
			if (!send_message(replica_fd, MessageType::REPL_PUT, payload)) {
				log_line("WARN", "Failed to send REPL_PUT to " + replica_info.node_id);
				close(replica_fd);
				continue; // try next
			}
			
			MessageType resp_type;
			string resp_payload;
			if (!recv_message(replica_fd, resp_type, resp_payload) || resp_type != MessageType::PUT_OK) {
				log_line("WARN", "Replica " + replica_info.node_id + " did not confirm PUT");
				close(replica_fd);
				continue; // try next replica
			}
			
			close(replica_fd);
			log_line("INFO", "Replica " + replica_info.node_id + " confirmed PUT");
			++successful_replicas;
		}
		
		// release locks
		for (const auto &key : acquired_locks) {
			release_lock(key);
		}
		
		// succeed as long as we stored on at least one node (self)
		send_message(client_fd, MessageType::PUT_OK, "replicated");
		log_line("INFO", "Chain replication completed: " + to_string(successful_replicas) + "/" + 
		         to_string(replication_factor) + " replicas for " + to_string(kv_pairs.size()) + " key(s)");
	} else {
		// non-primary replica just sends PUT_OK
		send_message(client_fd, MessageType::PUT_OK, "ok");
	}
}

// This reads keys locally.
void GTStoreStorage::handle_get(int client_fd, const string &payload) {
	vector<string> keys = split(payload, ';');
	if (keys.empty()) {
		send_message(client_fd, MessageType::ERROR, "no keys");
		return;
	}
	
	for (const auto &key : keys) {
		if (!key_valid(key)) {
			send_message(client_fd, MessageType::ERROR, "bad key: " + key);
			return;
		}
	}
	
	vector<string> values;
	for (const auto &key : keys) {
		auto it = kv_store.find(key);
		if (it == kv_store.end()) {
			log_line("WARN", "GET miss key=" + key + " on " + storage_id);
			send_message(client_fd, MessageType::ERROR, "missing: " + key); // this should happen only if key is not in gt store in general
			return;
		}
		log_line("INFO", "GET hit key=" + key + " value=" + it->second + " on " + storage_id);
		values.push_back(it->second);
	}
	
	string result;
	for (size_t i = 0; i < values.size(); ++i) {
		if (i > 0) result += ";";
		result += values[i];
	}
	send_message(client_fd, MessageType::GET_OK, result);
}

// This deletes keys locally.
void GTStoreStorage::handle_delete(int client_fd, const string &payload) {
	vector<string> keys = split(payload, ';');
	if (keys.empty()) {
		send_message(client_fd, MessageType::ERROR, "no keys");
		return;
	}

	// validate all keys
	for (const auto &key : keys) {
		if (!key_valid(key)) {
			send_message(client_fd, MessageType::ERROR, "bad key: " + key);
			return;
		}
	}
	
	for (const auto &key : keys) {
		auto it = kv_store.find(key);
		if (it == kv_store.end()) {
			log_line("WARN", "DELETE miss key=" + key + " on " + storage_id);
		} else {
			log_line("INFO", "DELETE key=" + key + " on " + storage_id);
			kv_store.erase(it);
		}
	}
	log_current_store();
	send_message(client_fd, MessageType::DELETE_OK, "ok");
}

// This accepts client requests.
void GTStoreStorage::serve_clients() {
	while (true) {
		int client_fd = accept_client(listen_fd);
		if (client_fd < 0) {
			continue;
		}
		thread([this, client_fd]() {
			MessageType type;
			string payload;
			if (!recv_message(client_fd, type, payload)) {
				close(client_fd);
				return;
			}
			
			bool is_paused = false;
			{
				lock_guard<mutex> guard(pause_mutex);
				is_paused = paused;
			}
			
			switch (type) {
				case MessageType::PAUSE_NODE:
					{
						lock_guard<mutex> guard(pause_mutex);
						paused = true;
					}
					log_line("INFO", "Node paused for rebalancing");
					send_message(client_fd, MessageType::PAUSE_ACK, "paused");
					close(client_fd);
					return;
					
				case MessageType::RESUME_NODE:
					{
						lock_guard<mutex> guard(pause_mutex);
						paused = false;
					}
					log_line("INFO", "Node resumed from rebalancing");
					send_message(client_fd, MessageType::RESUME_ACK, "resumed");
					close(client_fd);
					return;
					
				case MessageType::AVAILABILITY_CHECK:
					{
						bool is_available = false;
						{
							lock_guard<mutex> guard(lock_manager_mutex);
							is_available = key_locks.empty();
						}
						string status = is_available ? "yes" : "no";
						log_line("INFO", "Availability check: " + status + " (" + to_string(key_locks.size()) + " locks)");
						send_message(client_fd, MessageType::AVAILABLE_STATUS, status);
					}
					close(client_fd);
					return;
					
				case MessageType::CLIENT_PUT:
					if (is_paused) {
						log_line("WARN", "Rejecting CLIENT_PUT - node is paused for rebalancing");
						send_message(client_fd, MessageType::ERROR, "node_paused");
						close(client_fd);
						return;
					}
					handle_put(client_fd, payload, true);  // true = is_primary
					break;
					
				case MessageType::REPL_PUT:
					handle_put(client_fd, payload, false); // false = is_replica
					break;
					
				case MessageType::CLIENT_GET:
					if (is_paused) {
						log_line("WARN", "Rejecting CLIENT_GET - node is paused for rebalancing");
						send_message(client_fd, MessageType::ERROR, "node_paused");
						close(client_fd);
						return;
					}
					handle_get(client_fd, payload);
					break;
					
				case MessageType::MANAGER_GET:
					handle_get(client_fd, payload);
					break;
					
				case MessageType::MANAGER_DELETE:
					handle_delete(client_fd, payload);
					break;
					
				case MessageType::GET_ALL_KEYS:
					{
						// Manager requesting all keys for rebalancing
						string keys_payload;
						for (const auto &entry : kv_store) {
							if (!keys_payload.empty()) {
								keys_payload += ",";
							}
							keys_payload += entry.first;
						}
						log_line("INFO", "GET_ALL_KEYS request: returning " + to_string(kv_store.size()) + " keys");
						send_message(client_fd, MessageType::ALL_KEYS, keys_payload);
					}
					break;
					
				case MessageType::TABLE_PUSH:
					{
						// manager pushing updated routing table
						size_t parsed_factor = 0;
						auto nodes = parse_table_payload(payload, parsed_factor);
						routing_table = nodes;
						replication_factor = parsed_factor;
						log_line("INFO", "Received routing table with " + to_string(nodes.size()) + 
						         " nodes at replication " + to_string(parsed_factor));
						send_message(client_fd, MessageType::HEARTBEAT_ACK, "table_updated");
					}
					break;
					
				default:
					send_message(client_fd, MessageType::ERROR, "unknown");
					break;
			}
			close(client_fd);
		}).detach();
	}
}

// This starts the storage server work.
void GTStoreStorage::init() {
	
	cout << "Inside GTStoreStorage::init()\n";
	uint16_t storage_port = DEFAULT_STORAGE_BASE_PORT + (static_cast<uint16_t>(::getpid()) % 1000);
	const char *port_env = getenv("GTSTORE_STORAGE_PORT");
	if (port_env) {
		int parsed = atoi(port_env);
		if (parsed >= 0 && parsed <= 65535) {
			storage_port = static_cast<uint16_t>(parsed);
		}
	}
	string storage_addr = DEFAULT_STORAGE_HOST;
	const char *addr_env = getenv("GTSTORE_STORAGE_HOST");
	if (addr_env) {
		storage_addr = string(addr_env);
	}
	addr = NodeAddress{storage_addr, storage_port};


	const char *manager_host_env = getenv("GTSTORE_MANAGER_HOST");
	string manager_host = DEFAULT_MANAGER_HOST;
	if (manager_host_env) {
		manager_host = string(manager_host_env);
	}

	const char *manager_port_env = getenv("GTSTORE_MANAGER_PORT");
	uint16_t manager_port = DEFAULT_MANAGER_PORT;
	if (manager_port_env) {
		int parsed = atoi(manager_port_env);
		if (parsed >= 0 && parsed <= 65535) {
			manager_port = static_cast<uint16_t>(parsed);
		}
	}
	manager_addr = NodeAddress{manager_host, manager_port};

	if (!kv_store.empty()) {
		kv_store.clear();
	}
	const char *label = getenv("GTSTORE_NODE_LABEL");
	if (label && *label) {
		storage_id = label;
	} else {
		storage_id = "node" + to_string(::getpid());
	}
	replication_factor = 1;
	running = true;
	paused = false;
	setup_logging(COMPONENT_PREFIX + storage_id);
	log_line("INFO", "Storage label set to " + storage_id);
	
	listen_fd = create_listen_socket(addr, BACKLOG);
	if (listen_fd < 0) {
		log_line("ERROR", "storage listen failed");
		return;
	}
	log_line("INFO", "Listening on " + addr.host + ":" + to_string(addr.port));
	if (!register_with_manager()) {
		log_line("ERROR", "storage registration with manager failed");
		return;
	}
	heartbeat_thread = thread(&GTStoreStorage::heartbeat_loop, this);
	heartbeat_thread.detach();
	serve_clients();
}

// This prints every key/value in this storage.
void GTStoreStorage::log_current_store() {
	ostringstream out;
	out << "Store snapshot on " << storage_id << ":";
	for (const auto &entry : kv_store) {
		out << " [" << entry.first << "=" << entry.second << "]";
	}
	log_line("INFO", out.str());
}

// This tries to acquire a write lock on a key.
bool GTStoreStorage::try_acquire_lock(const string &key, const string &client_id) {
	lock_guard<mutex> guard(lock_manager_mutex);
	auto it = key_locks.find(key);
	if (it != key_locks.end()) {
		// Key is already locked by another client
		return false;
	}
	key_locks[key] = client_id;
	log_line("INFO", "Lock acquired for key=" + key + " by client=" + client_id);
	return true;
}

// This releases a write lock on a key.
void GTStoreStorage::release_lock(const string &key) {
	lock_guard<mutex> guard(lock_manager_mutex);
	auto it = key_locks.find(key);
	if (it != key_locks.end()) {
		log_line("INFO", "Lock released for key=" + key + " by client=" + it->second);
		key_locks.erase(it);
	}
}

int main(int argc, char **argv) {

	GTStoreStorage storage;
	storage.init();
	
}
