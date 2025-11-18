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
		heartbeat_times[info.node_id] = std::chrono::steady_clock::now();
		std::sort(node_table.begin(), node_table.end(), [](const StorageNodeInfo &lhs, const StorageNodeInfo &rhs) {
			return lhs.token < rhs.token;
		});
	}
	log_line("INFO", "Registered storage " + info.node_id + " at " + info.address.host + ":" + std::to_string(info.address.port));
	log_line("INFO", "Routing table snapshot: " + describe_nodes(snapshot_nodes()));
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

// This drops nodes that stopped sending heartbeats.
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
					removed.emplace_back(it->node_id, seconds_since);
					heartbeat_times.erase(it->node_id);
					it = node_table.erase(it);
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
