#include "gtstore.hpp"
#include "utils.hpp"

#include <thread>

using namespace gtstore_utils;

namespace {
const std::string COMPONENT_PREFIX = "storage_";
const int BACKLOG = 16;
}

// This tells manager about this storage node.
void GTStoreStorage::register_with_manager() {
	NodeAddress manager_addr{DEFAULT_MANAGER_HOST, DEFAULT_MANAGER_PORT};
	int fd = connect_to_host(manager_addr);
	if (fd < 0) {
		log_line("ERROR", "could not reach manager");
		return;
	}
	std::string payload = storage_id + ",127.0.0.1," + std::to_string(listen_port);
	if (!send_message(fd, MessageType::STORAGE_REGISTER, payload)) {
		log_line("ERROR", "failed to send register");
		close(fd);
		return;
	}
	MessageType type;
	std::string table_payload;
	if (recv_message(fd, type, table_payload) && type == MessageType::TABLE_PUSH) {
		size_t parsed_factor = 1;
		auto nodes = parse_table_payload(table_payload, parsed_factor);
		replication_factor = parsed_factor;
		log_line("INFO", "Received table with " + std::to_string(nodes.size()) + " nodes at replication " + std::to_string(replication_factor));
	}
	close(fd);
}

// This sends heartbeat messages to manager.
void GTStoreStorage::heartbeat_loop() {
	while (running) {
		std::this_thread::sleep_for(std::chrono::seconds(2));
		NodeAddress manager_addr{DEFAULT_MANAGER_HOST, DEFAULT_MANAGER_PORT};
		int fd = connect_to_host(manager_addr);
		if (fd < 0) {
			continue;
		}
		if (!send_message(fd, MessageType::HEARTBEAT, storage_id)) {
			close(fd);
			continue;
		}
		MessageType type;
		std::string payload;
		recv_message(fd, type, payload);
		close(fd);
	}
}

// This checks the key size.
bool GTStoreStorage::key_valid(const std::string &key) {
	return !key.empty() && key.size() <= MAX_KEY_BYTE_PER_REQUEST;
}

// This checks the value size.
bool GTStoreStorage::value_valid(const std::string &value) {
	return value.size() <= MAX_VALUE_BYTE_PER_REQUEST;
}

// This stores a key locally.
void GTStoreStorage::handle_put(int client_fd, const std::string &payload) {
	auto pos = payload.find('|');
	if (pos == std::string::npos) {
		send_message(client_fd, MessageType::ERROR, "bad put");
		return;
	}
	std::string key = payload.substr(0, pos);
	std::string value = payload.substr(pos + 1);
	if (!key_valid(key)) {
		send_message(client_fd, MessageType::ERROR, "bad key");
		return;
	}
	if (!value_valid(value)) {
		send_message(client_fd, MessageType::ERROR, "bad value");
		return;
	}
	kv_store[key] = value;
	send_message(client_fd, MessageType::PUT_OK, "ok");
}

// This reads a key locally.
void GTStoreStorage::handle_get(int client_fd, const std::string &payload) {
	if (!key_valid(payload)) {
		send_message(client_fd, MessageType::ERROR, "bad key");
		return;
	}
	auto it = kv_store.find(payload);
	if (it == kv_store.end()) {
		send_message(client_fd, MessageType::ERROR, "missing");
		return;
	}
	send_message(client_fd, MessageType::GET_OK, it->second);
}

// This accepts client requests.
void GTStoreStorage::serve_clients() {
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
			if (type == MessageType::CLIENT_PUT) {
				handle_put(client_fd, payload);
			} else if (type == MessageType::CLIENT_GET) {
				handle_get(client_fd, payload);
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
	listen_port = DEFAULT_STORAGE_BASE_PORT + (static_cast<uint16_t>(::getpid()) % 1000);
	if (!kv_store.empty()) {
		kv_store.clear();
	}
	storage_id = "node" + std::to_string(::getpid());
	replication_factor = 1;
	running = true;
	setup_logging(COMPONENT_PREFIX + storage_id);
	NodeAddress addr{"127.0.0.1", listen_port};
	listen_fd = create_listen_socket(addr, BACKLOG);
	if (listen_fd < 0) {
		log_line("ERROR", "storage listen failed");
		return;
	}
	log_line("INFO", "Listening on " + addr.host + ":" + std::to_string(addr.port));
	register_with_manager();
	heartbeat_thread = std::thread(&GTStoreStorage::heartbeat_loop, this);
	heartbeat_thread.detach();
	serve_clients();
}

int main(int argc, char **argv) {

	GTStoreStorage storage;
	storage.init();
	
}
