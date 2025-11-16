#ifndef GTSTORE
#define GTSTORE

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <sys/wait.h>

#include "net_common.hpp"

const std::string DEFAULT_MANAGER_HOST = "127.0.0.1";
const uint16_t DEFAULT_MANAGER_PORT = 5000;
const uint16_t DEFAULT_STORAGE_BASE_PORT = 6000;

#define MAX_KEY_BYTE_PER_REQUEST 20
#define MAX_VALUE_BYTE_PER_REQUEST 1000

using namespace std;

typedef vector<string> val_t;

class GTStoreClient {
	private:
		int client_id;
		val_t value;
		vector<StorageNodeInfo> routing_table;
		NodeAddress manager_address;
		size_t replication_factor;
		StorageNodeInfo pick_primary(const string &key);
		StorageNodeInfo pick_node_for_attempt(const string &key, size_t attempt);
		val_t parse_value(const string &payload);
		string serialize_value(const val_t &value);
		bool refresh_table();
		bool validate_key(const string &key);
		bool validate_value(const val_t &value);
	public:
		GTStoreClient();
		void init(int id);
		void finalize();
		val_t get(string key);
		bool put(string key, val_t value);
		std::vector<StorageNodeInfo> current_table_snapshot() const;
		StorageNodeInfo debug_pick_for_test(const std::string &key, size_t attempt);
		size_t current_replication() const;
};

class GTStoreManager {
	private:
		uint16_t listen_port;
		int listen_fd;
		vector<StorageNodeInfo> node_table;
		size_t replication_factor;
		std::mutex table_mutex;
		std::unordered_map<std::string, std::chrono::steady_clock::time_point> heartbeat_times;
		std::thread heartbeat_thread;
		bool running;
		void accept_loop();
		void handle_storage_register(const string &payload);
		void handle_heartbeat(const string &payload);
		vector<StorageNodeInfo> snapshot_nodes();
		void monitor_heartbeats();
	public:
		void init();
};

class GTStoreStorage {
	private:
		uint16_t listen_port;
		int listen_fd;
		unordered_map<string, string> kv_store;
		string storage_id;
		size_t replication_factor;
		std::thread heartbeat_thread;
		bool running;
		void register_with_manager();
		void serve_clients();
		void handle_put(int client_fd, const string &payload);
		void handle_get(int client_fd, const string &payload);
		bool key_valid(const std::string &key);
		bool value_valid(const std::string &value);
		void heartbeat_loop();
	public:
		void init();
};

#endif
