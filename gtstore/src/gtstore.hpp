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

#define MAX_KEY_BYTE_PER_REQUEST 20
#define MAX_VALUE_BYTE_PER_REQUEST 1000
#define DEFAULT_MANAGER_HOST "127.0.0.1"
#define DEFAULT_MANAGER_PORT 5000
#define DEFAULT_STORAGE_HOST "127.0.0.1"
#define DEFAULT_STORAGE_BASE_PORT 6000

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
		void init(int id, const string &manager_host = DEFAULT_MANAGER_HOST, uint16_t manager_port = DEFAULT_MANAGER_PORT);
		void finalize();
		val_t get(string key);
		bool put(string key, val_t value);
		vector<StorageNodeInfo> current_table_snapshot() const;
		StorageNodeInfo debug_pick_for_test(const string &key, size_t attempt);
		size_t current_replication() const;
};

class GTStoreManager {
	private:
		NodeAddress addr;
		int listen_fd;
		vector<StorageNodeInfo> node_table;
		size_t replication_factor;
		mutex node_table_mutex;
		unordered_map<string, chrono::steady_clock::time_point> heartbeat_times;
		mutex heartbeat_mutex;
		thread heartbeat_thread;
		bool running;
		void accept_loop();
		bool send_table(int client_fd, const vector<StorageNodeInfo> &nodes, size_t replication_factor);
		StorageNodeInfo handle_storage_register(const string &payload);
		void handle_heartbeat(const string &payload);
		vector<StorageNodeInfo> snapshot_nodes();
		void broadcast_table_to_storage_nodes();
		void monitor_heartbeats();
		void rebalance_on_node_failure(int failed_idx, uint64_t failed_token, const vector<StorageNodeInfo> &nodes);
		void rebalance_on_node_join(uint64_t new_token);
		int find_node_index(const string &node_id) const;
		vector<string> get_all_keys_from_node(const NodeAddress &addr);
		void replicate_key_to_node(const vector<string> &keys, const vector<string> &values, const NodeAddress &dest_addr);
		vector<string> get_key_from_node(const vector<string> &keys, const NodeAddress &addr);
		void delete_key_from_node(const vector<string> &keys, const NodeAddress &addr);
		void delete_key_from_node(const string &key, const NodeAddress &addr);
		bool pause_node(const NodeAddress &addr);
		bool resume_node(const NodeAddress &addr);
		bool wait_for_availability(const NodeAddress &addr, int max_attempts = 30);
	public:
		void init();
};

class GTStoreStorage {
	private:
		NodeAddress addr;
		NodeAddress manager_addr;
		int listen_fd;
		unordered_map<string, string> kv_store;
		string storage_id;
		size_t replication_factor;
		vector<StorageNodeInfo> routing_table;
		thread heartbeat_thread;
		bool running;
		bool paused;
		mutex lock_manager_mutex;
		mutex pause_mutex;
		unordered_map<string, string> key_locks;  // maps key -> client_id holding lock
		bool register_with_manager();
		void serve_clients();
		void handle_put(int client_fd, const string &payload, bool is_primary);
		void handle_get(int client_fd, const string &payload);
		void handle_delete(int client_fd, const string &payload);
		bool key_valid(const string &key);
		bool value_valid(const string &value);
		void heartbeat_loop();
		void log_current_store();
		bool try_acquire_lock(const string &key, const string &client_id);
		void release_lock(const string &key);
	public:
		void init();
};

#endif
