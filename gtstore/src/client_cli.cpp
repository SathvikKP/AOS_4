#include "gtstore.hpp"

#include <cstring>
#include <iostream>

using namespace std;

// This shows how to call the cli.
static void print_usage(const char *prog) {
    cout << "Usage: " << prog << " (--get <key> | --put <key> --val <value>) [--manager-host <host>] [--manager-port <port>]" << endl;
}

// This runs the cli actions.
int main(int argc, char **argv) {
    string manager_host = DEFAULT_MANAGER_HOST;
    uint16_t manager_port = DEFAULT_MANAGER_PORT;
    string key;
    string value_str;
    bool do_get = false;
    bool do_put = false;

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--get" && i + 1 < argc) {
            do_get = true;
            key = argv[++i];
        } else if (arg == "--put" && i + 1 < argc) {
            do_put = true;
            key = argv[++i];
        } else if (arg == "--val" && i + 1 < argc) {
            value_str = argv[++i];
        } else if (arg == "--manager-host" && i + 1 < argc) {
            manager_host = argv[++i];
        } else if (arg == "--manager-port" && i + 1 < argc) {
            manager_port = static_cast<uint16_t>(stoi(argv[++i]));
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (key.empty() || (do_get == do_put) || (do_put && value_str.empty())) {
        print_usage(argv[0]);
        return 1;
    }

    GTStoreClient client;
    client.init(0, manager_host, manager_port);

    bool ok = true;
    if (do_put) {
        val_t value;
        value.push_back(value_str);
        ok = client.put(key, value);
    } else {
        val_t value = client.get(key);
        ok = !value.empty();
    }

    client.finalize();
    return ok ? 0 : 2;
}
