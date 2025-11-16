#include "net_common.hpp"

#include <cerrno>
#include <cstring>
#include <iostream>

// This sends every byte using blocking retries.
bool send_all(int fd, const void *data, size_t length) {
    const uint8_t *buffer = static_cast<const uint8_t *>(data);
    size_t sent_total = 0;
    while (sent_total < length) {
        ssize_t sent = send(fd, buffer + sent_total, length - sent_total, 0);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "send failed: " << std::strerror(errno) << "\n";
            return false;
        }
        if (sent == 0) {
            return false;
        }
        sent_total += static_cast<size_t>(sent);
    }
    return true;
}

// This reads the requested number of bytes.
bool recv_all(int fd, void *data, size_t length) {
    uint8_t *buffer = static_cast<uint8_t *>(data);
    size_t recv_total = 0;
    while (recv_total < length) {
        ssize_t got = recv(fd, buffer + recv_total, length - recv_total, 0);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "recv failed: " << std::strerror(errno) << "\n";
            return false;
        }
        if (got == 0) {
            return false;
        }
        recv_total += static_cast<size_t>(got);
    }
    return true;
}

// This sends a header followed by payload.
bool send_message(int fd, MessageType type, const std::string &payload) {
    MessageHeader header{};
    header.type = htons(static_cast<uint16_t>(type));
    header.reserved = 0;
    header.payload_size = htonl(static_cast<uint32_t>(payload.size()));

    if (!send_all(fd, &header, sizeof(header))) {
        return false;
    }
    if (!payload.empty()) {
        return send_all(fd, payload.data(), payload.size());
    }
    return true;
}

// This receives a header and payload.
bool recv_message(int fd, MessageType &type, std::string &payload) {
    MessageHeader header{};
    if (!recv_all(fd, &header, sizeof(header))) {
        return false;
    }

    uint16_t raw_type = ntohs(header.type);
    uint32_t payload_len = ntohl(header.payload_size);
    type = static_cast<MessageType>(raw_type);

    payload.clear();
    if (payload_len == 0) {
        return true;
    }

    payload.resize(payload_len);
    if (!recv_all(fd, &payload[0], payload.size())) {
        payload.clear();
        return false;
    }
    return true;
}

// This opens a blocking client socket.
int connect_to_host(const NodeAddress &address) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << "\n";
        return -1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(address.port);
    if (inet_pton(AF_INET, address.host.c_str(), &dest.sin_addr) <= 0) {
        std::cerr << "inet_pton failed for " << address.host << "\n";
        close(fd);
        return -1;
    }

    if (connect(fd, reinterpret_cast<sockaddr *>(&dest), sizeof(dest)) < 0) {
        std::cerr << "connect failed: " << std::strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    return fd;
}

// This creates a listening socket for servers.
int create_listen_socket(const NodeAddress &address, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << "\n";
        return -1;
    }

    int enable = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0) {
        std::cerr << "setsockopt failed: " << std::strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(address.port);
    if (inet_pton(AF_INET, address.host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "inet_pton failed for " << address.host << "\n";
        close(fd);
        return -1;
    }

    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::cerr << "bind failed: " << std::strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    if (listen(fd, backlog) < 0) {
        std::cerr << "listen failed: " << std::strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    return fd;
}

// This accepts a waiting client.
int accept_client(int listen_fd) {
    sockaddr_in addr{};
    socklen_t addr_len = sizeof(addr);
    int client_fd = accept(listen_fd, reinterpret_cast<sockaddr *>(&addr), &addr_len);
    if (client_fd < 0) {
        std::cerr << "accept failed: " << std::strerror(errno) << "\n";
        return -1;
    }
    return client_fd;
}
