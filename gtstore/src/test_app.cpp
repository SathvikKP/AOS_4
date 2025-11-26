#include "gtstore.hpp"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

namespace {
const vector<pair<string, string>> FAILURE_KEYS = {
	{"key1", "value1"},
	{"key2", "value2"},
	{"key3", "value3"},
	{"key4", "value4"},
	{"key5", "value5"},
	{"key6", "value6"}
};

// This appends a performance line when requested.
void append_perf_line(const std::string &line) {
	cout << line << "\n";
	const char *path = std::getenv("GTSTORE_PERF_FILE");
	if (path && *path) {
		std::ofstream out(path, std::ios::app);
		if (out.is_open()) {
			out << line << "\n";
		}
	}
}

// This prints a simple usage hint.
void print_usage(const char *prog) {
	cout << "Usage: " << prog << " <test> <client_id> [extra]\n";
	cout << "Tests: single_set_get, basic_trace, failure_load, failure_verify, multi_failure_load, multi_failure_verify, throughput, load_balance\n";
}
}

// This runs the simple put/get smoke test.
void single_set_get(int client_id) {
	cout << "Testing single set-get for GTStore by client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	string key = to_string(client_id);
	vector<string> value;
	value.push_back("phone");
	value.push_back("phone_case");
	client.put(key, value);
	client.get(key);
	client.finalize();
}

// This runs the Test 1 trace of puts and gets.
void basic_trace(int client_id) {
	cout << "Running basic trace test with client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	vector<string> value;
	value.push_back("value1");
	client.put("key1", value);
	val_t got = client.get("key1");
	if (!got.empty()) {
		cout << "Trace get key1 => " << got[0] << "\n";
	}
	value.clear();
	value.push_back("value2");
	client.put("key1", value);
	value.clear();
	value.push_back("value3");
	client.put("key2", value);
	value.clear();
	value.push_back("value4");
	client.put("key3", value);
	got = client.get("key1");
	if (!got.empty()) {
		cout << "Trace get key1 latest => " << got[0] << "\n";
	}
	got = client.get("key2");
	if (!got.empty()) {
		cout << "Trace get key2 => " << got[0] << "\n";
	}
	got = client.get("key3");
	if (!got.empty()) {
		cout << "Trace get key3 => " << got[0] << "\n";
	}
	client.finalize();
}

// This loads keys ahead of the single node failure test.
void failure_load(int client_id) {
	cout << "Loading keys for failure test using client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	for (const auto &entry : FAILURE_KEYS) {
		val_t value;
		value.push_back(entry.second);
		client.put(entry.first, value);
		cout << "Stored " << entry.first << " => " << entry.second << "\n";
	}
	client.finalize();
}

// This verifies keys after a single node failure.
void failure_verify(int client_id) {
	cout << "Verifying keys after failure with client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	for (const auto &entry : FAILURE_KEYS) {
		val_t got = client.get(entry.first);
		if (!got.empty()) {
			cout << "Read " << entry.first << " => " << got[0] << "\n";
		} else {
			cout << "Missing key " << entry.first << " after failure\n";
		}
	}
	client.finalize();
}

// This loads many keys and overwrites a few for the multi-failure test.
void multi_failure_load(int client_id) {
	cout << "Loading keys for multi failure test using client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	for (int i = 0; i < 20; ++i) {
		string key = "many_key_" + to_string(i);
		val_t value;
		value.push_back("value_" + to_string(i));
		client.put(key, value);
	}
	vector<int> overwrite = {2, 5, 9};
	for (int idx : overwrite) {
		string key = "many_key_" + to_string(idx);
		val_t value;
		value.push_back("updated_" + to_string(idx));
		client.put(key, value);
	}
	client.finalize();
}

// This verifies the multi failure data set.
void multi_failure_verify(int client_id) {
	cout << "Verifying keys after multi failure using client " << client_id << ".\n";
	GTStoreClient client;
	client.init(client_id);
	for (int i = 0; i < 20; ++i) {
		string key = "many_key_" + to_string(i);
		val_t got = client.get(key);
		if (!got.empty()) {
			cout << "Read " << key << " => " << got[0] << "\n";
		}
	}
	client.finalize();
}

// This runs the throughput benchmark.
void throughput_driver(int client_id, int total_ops) {
	cout << "Running throughput test with " << total_ops << " ops.\n";
	GTStoreClient client;
	client.init(client_id);
	vector<string> keys;
	for (int i = 0; i < 100; ++i) {
		keys.push_back("tp_key_" + to_string(i));
	}
	std::mt19937 rng(client_id);
	std::uniform_int_distribution<int> pick(0, static_cast<int>(keys.size() - 1));
	auto start = std::chrono::steady_clock::now();
	for (size_t i = 0; i < keys.size(); ++i) {
		val_t value;
		value.push_back("tp_val_" + to_string(i));
		client.put(keys[i], value);
	}
	
	for (int i = 0; i < total_ops; ++i) {
		string key = keys[pick(rng)];
		if (i % 2 == 0) {
			val_t value;
			value.push_back("tp_val_" + to_string(i));
			client.put(key, value);
		} else {
			client.get(key);
		}
	}
	auto finish = std::chrono::steady_clock::now();
	double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(finish - start).count();
	if (seconds <= 0.0) {
		seconds = 1e-6;
	}
	double ops_per_sec = static_cast<double>(total_ops) / seconds;
	std::ostringstream line;
	line << client.current_replication() << "," << total_ops << "," << seconds << "," << ops_per_sec;
	append_perf_line(line.str());
	client.finalize();
}

// This runs the load balance histogram test.
void load_balance_driver(int client_id, int inserts) {
	cout << "Running load balance test with " << inserts << " inserts.\n";
	GTStoreClient client;
	client.init(client_id);
	auto table = client.current_table_snapshot();
	if (table.empty()) {
		cout << "No storage nodes available for load test.\n";
		client.finalize();
		return;
	}
	std::unordered_map<std::string, size_t> counts;
	std::unordered_set<std::string> unique_nodes;
	for (const auto &node : table) {
		if (unique_nodes.find(node.node_id) == unique_nodes.end()) {
			counts[node.node_id] = 0;
			unique_nodes.insert(node.node_id);
		}
	}
	for (int i = 0; i < inserts; ++i) {
		string key = "lb_key_" + to_string(i);
		val_t value;
		value.push_back("lb_val_" + to_string(i));
		client.put(key, value);
		StorageNodeInfo owner = client.debug_pick_for_test(key, 0);
		counts[owner.node_id] += 1;
	}
	for (const auto &node_id : unique_nodes) {
		std::ostringstream line;
		line << node_id << "," << counts[node_id];
		append_perf_line(line.str());
		cout << "Load count for " << node_id << ": " << counts[node_id] << "\n";
	}
	client.finalize();
}

int main(int argc, char **argv) {
	if (argc < 3) {
		print_usage(argv[0]);
		return 1;
	}
	string test = string(argv[1]);
	int client_id = atoi(argv[2]);
	if (test == "single_set_get") {
		single_set_get(client_id);
	} else if (test == "basic_trace") {
		basic_trace(client_id);
	} else if (test == "failure_load") {
		failure_load(client_id);
	} else if (test == "failure_verify") {
		failure_verify(client_id);
	} else if (test == "multi_failure_load") {
		multi_failure_load(client_id);
	} else if (test == "multi_failure_verify") {
		multi_failure_verify(client_id);
	} else if (test == "throughput") {
		int total_ops = (argc >= 4) ? atoi(argv[3]) : 200000;
		throughput_driver(client_id, total_ops);
	} else if (test == "load_balance") {
		int inserts = (argc >= 4) ? atoi(argv[3]) : 100000;
		load_balance_driver(client_id, inserts);
	} else {
		print_usage(argv[0]);
		return 1;
	}
	return 0;
}
