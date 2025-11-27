CFLAGS  =
LFLAGS  =
CC      = g++ -std=c++11
RM      = /bin/rm -rf
BIN_DIR = bin
COMMON_SRC = src/net_common.cpp src/utils.cpp

TESTS = test_app manager storage client
CLIENT_SRC = src/test_app.cpp src/client.cpp

all: $(TESTS)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

manager: src/manager.cpp $(COMMON_SRC) | $(BIN_DIR)
	$(CC) -Wall src/manager.cpp $(COMMON_SRC) -o $(BIN_DIR)/manager

storage: src/storage.cpp $(COMMON_SRC) | $(BIN_DIR)
	$(CC) -Wall src/storage.cpp $(COMMON_SRC) -o $(BIN_DIR)/storage

test_app: $(CLIENT_SRC) $(COMMON_SRC) | $(BIN_DIR)
	$(CC) -Wall $(CLIENT_SRC) $(COMMON_SRC) -o $(BIN_DIR)/test_app

client: src/client_cli.cpp src/client.cpp $(COMMON_SRC) | $(BIN_DIR)
	$(CC) -Wall src/client_cli.cpp src/client.cpp $(COMMON_SRC) -o $(BIN_DIR)/client

clean:
	$(RM) *.o $(BIN_DIR)
