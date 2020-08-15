#include <iostream>
#include <chrono>

#include <mini_bus.hpp>

using namespace mini_bus;
struct AutoReconnectConnection : ConnectionInfo {
  io_service &service;
  ip::tcp::endpoint endpoint;
  std::chrono::seconds delay;
  std::function<void(MiniBusClient &)> on_connected;

  AutoReconnectConnection(
      io_service &service, ip::tcp::endpoint const &endpoint, std::chrono::seconds delay,
      std::function<void(MiniBusClient &)> const &on_connected)
      : service(service), endpoint(endpoint), delay(delay), on_connected(on_connected) {}

  std::optional<ip::tcp::socket> create_connected_socket() override {
    try {
      ip::tcp::socket ret{service};
      std::cout << "connecting to socket" << std::endl;
      ret.connect(endpoint);
      std::cout << "connected to socket, handshake" << std::endl;
      return {std::move(ret)};
    } catch (...) { return std::nullopt; }
  }
  void connected(MiniBusClient *client) override {
    std::cout << "minibus connected" << std::endl;
    on_connected(*client);
    std::cout << "setup finish" << std::endl;
  }

  bool disconnected(bool imm) override {
    std::cout << "minibus disconnected" << std::endl;
    std::this_thread::sleep_for(delay);
    std::cout << "reconnecting" << std::endl;
    return true;
  }
};

int main(int argc, char *argv[]) {
  using namespace std::chrono_literals;
  try {
    io_service service;
    MiniBusClient client{std::make_shared<AutoReconnectConnection>(
        service, ip::tcp::endpoint{ip::address::from_string("127.0.0.1"), 4040}, 5s, [](MiniBusClient &client) {
          client.register_handler("echo", [](auto inp) { return std::string{inp}; });
          client.set("registry", "demo", "(value will be ignored)")->wait();
          client.set_private("key", "value")->wait();
        })};
    while (true) {
      std::this_thread::sleep_for(1s);
      try {
        client.notify("event", "notify");
      } catch (...) {
        std::cout << "failed to send notify" << std::endl;
      }
    }
  } catch (std::exception &e) { std::cout << e.what() << std::endl; }
}