#include <iostream>
#include <chrono>

#include <mini_bus.hpp>

int main(int argc, char *argv[]) {
  using namespace mini_bus;
  using namespace std::chrono_literals;
  try {
    io_service service;
    MiniBusClient client{service, ip::address::from_string("127.0.0.1"), 4040};
    client.register_handler("echo", [](auto inp) { return std::string{inp}; });
    client.set("registry", "demo", "(value will be ignored)");
    client.set_private("key", "value");
    while (true) {
      std::this_thread::sleep_for(1s);
      client.notify("event", "notify");
    }
  } catch (std::exception &e) { std::cout << e.what() << std::endl; }
}