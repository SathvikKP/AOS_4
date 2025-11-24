#ifndef GTSTORE_NET_COMMON_HPP
#define GTSTORE_NET_COMMON_HPP

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

// shared message type ids
enum class MessageType : uint16_t {
    CLIENT_PUT = 1,
    CLIENT_GET = 2,
    PUT_OK = 3,
    GET_OK = 4,
    ERROR = 5,
    REPL_PUT = 6,
    REPL_ACK = 7,
    HEARTBEAT = 8,
    HEARTBEAT_ACK = 9,
    TABLE_PUSH = 10,
    STORAGE_REGISTER = 11,
    CLIENT_HELLO = 12,
    REPL_CONFIRM = 13,
    GET_ALL_KEYS = 14,
    ALL_KEYS = 15,
    DELETE_OK = 17,
    PAUSE_NODE = 18,
    RESUME_NODE = 19,
    PAUSE_ACK = 20,
    RESUME_ACK = 21,
    AVAILABILITY_CHECK = 22,
    AVAILABLE_STATUS = 23,
    MANAGER_GET = 24,
    MANAGER_DELETE = 25
};

// compact header carried before each payload
struct MessageHeader {
    uint16_t type;
    uint16_t reserved;
    uint32_t payload_size;
};

// describes a TCP endpoint
struct NodeAddress {
    std::string host;
    uint16_t port;
};

// tracks a storage node entry
struct StorageNodeInfo {
    std::string node_id;
    NodeAddress address;
    uint64_t token;
};

// This sends every byte in the given buffer.
bool send_all(int fd, const void *data, size_t length);

// This reads exact number of bytes into buffer.
bool recv_all(int fd, void *data, size_t length);

// This sends a typed message with payload.
bool send_message(int fd, MessageType type, const std::string &payload);

// This reads a typed message with payload.
bool recv_message(int fd, MessageType &type, std::string &payload);

// This opens a client socket to host:port.
int connect_to_host(const NodeAddress &address);

// This creates, binds, and listens on host:port.
int create_listen_socket(const NodeAddress &address, int backlog);

// This accepts a pending client connection.
int accept_client(int listen_fd);

#endif
