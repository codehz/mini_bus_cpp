#include <iostream>

#include <mini_bus.hpp>

int main(int argc, char *argv[]) {
  try {
    io_service service;
    MiniBusClient client{service, ip::address::from_string("127.0.0.1"), 4040};
    client.register_handler("echo", [](auto inp) { return inp; });
    client.set("registry", "demo", "(value will be ignored)");
    client.join();
  } catch (std::exception &e) { std::cout << e.what() << std::endl; }
}