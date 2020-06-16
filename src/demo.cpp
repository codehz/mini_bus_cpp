#include <iostream>

#include <mini_bus.hpp>

int main(int argc, char *argv[]) {
  try {
    io_service service;
    MiniBusClient client{service, ip::address::from_string("127.0.0.1"), 4040};
    std::cout << "connected" << std::endl;

    client.observe("shared", "key", [](std::string_view sv) { std::cout << "update: " << sv << std::endl; });

    client.ping("test");
    client.set("shared", "key", "value");
    std::cout << "v: " << client.get("shared", "key") << std::endl;

    std::cout << "here" << std::endl;

    for (auto &[k, v] : client.keys("shared")) { std::cout << "kv: " << v << (int) k << std::endl; }

    std::cout << "waiting" << std::endl;

    NotifyToken<int> tok;
    client.observe("registry", "demo", [&](auto sv) { tok.notify(0); });

    tok.wait();

    std::cout << "call" << client.call("demo", "echo", "boom") << std::endl;
  } catch (std::exception &e) { std::cout << e.what() << std::endl; }
}