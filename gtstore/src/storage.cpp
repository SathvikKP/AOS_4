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

// This stores a key locally.
void GTStoreStorage::handle_put(int client_fd, const string &payload, bool is_primary) {
	auto pos = payload.find('|');
	if (pos == string::npos) {
		send_message(client_fd, MessageType::ERROR, "bad put");
		return;
	}
	string key = payload.substr(0, pos);
	string value = payload.substr(pos + 1);
	if (!key_valid(key)) {
		send_message(client_fd, MessageType::ERROR, "bad key");
		return;
	}
	if (!value_valid(value)) {
		send_message(client_fd, MessageType::ERROR, "bad value");
		return;
	}
	
	// Only primary needs to acquire lock
	string client_id = "client_" + to_string(client_fd);
	if (is_primary) {
		if (!try_acquire_lock(key, client_id)) {
			send_message(client_fd, MessageType::ERROR, "locked");
			log_line("WARN", "PUT rejected key=" + key + " (locked) on " + storage_id);
			return;
		}
	}
	
	log_line("INFO", "PUT key=" + key + " value=" + value + " on " + storage_id);
	kv_store[key] = value;
	log_current_store();
	
	// Send PUT_OK
	send_message(client_fd, MessageType::PUT_OK, "ok");
	
	// Only primary waits for REPL_CONFIRM
	if (is_primary) {
		MessageType confirm_type;
		string confirm_payload;
		if (recv_message(client_fd, confirm_type, confirm_payload) && 
		    confirm_type == MessageType::REPL_CONFIRM) {
			log_line("INFO", "Replication confirmed for key=" + key);
			release_lock(key);
			send_message(client_fd, MessageType::PUT_OK, "replicated");
		} else {
			log_line("WARN", "No replication confirmation for key=" + key);
			release_lock(key);
			send_message(client_fd, MessageType::ERROR, "no_confirmation");
		}
	}
}

// This reads a key locally.
void GTStoreStorage::handle_get(int client_fd, const string &payload) {
	if (!key_valid(payload)) {
		send_message(client_fd, MessageType::ERROR, "bad key");
		return;
	}
	auto it = kv_store.find(payload);
	if (it == kv_store.end()) {
		log_line("WARN", "GET miss key=" + payload + " on " + storage_id);
		send_message(client_fd, MessageType::ERROR, "missing"); // this should happen only if key is not in gt store in general
		return;
	}
	log_line("INFO", "GET hit key=" + payload + " value=" + it->second + " on " + storage_id);
	send_message(client_fd, MessageType::GET_OK, it->second);
}

// This deletes a key locally.
void GTStoreStorage::handle_delete(int client_fd, const string &payload) {
	if (!key_valid(payload)) {
		send_message(client_fd, MessageType::ERROR, "bad key");
		return;
	}
	auto it = kv_store.find(payload);
	if (it == kv_store.end()) {
		log_line("WARN", "DELETE miss key=" + payload + " on " + storage_id);
		send_message(client_fd, MessageType::DELETE_OK, "not_found");
		return;
	}
	log_line("INFO", "DELETE key=" + payload + " on " + storage_id);
	kv_store.erase(it);
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
			if (type == MessageType::CLIENT_PUT) {
				handle_put(client_fd, payload, true);  // true = is_primary
			} else if (type == MessageType::REPL_PUT) {
				handle_put(client_fd, payload, false); // false = is_replica
			} else if (type == MessageType::CLIENT_GET) {
				handle_get(client_fd, payload);
			} else if (type == MessageType::CLIENT_DELETE) {
				handle_delete(client_fd, payload);
			} else if (type == MessageType::GET_ALL_KEYS) {
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
			} else {
				send_message(client_fd, MessageType::ERROR, "unknown");
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
