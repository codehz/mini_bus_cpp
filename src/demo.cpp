#include <iostream>
#include <chrono>

#include <mini_bus.hpp>

int main(int argc, char *argv[]) {
  using namespace mini_bus;
  using namespace std::chrono_literals;
  try {
    io_service service;
    MiniBusClient client{service, ip::address::from_string("127.0.0.1"), 4040};
    std::cout << "connected" << std::endl;

    client.observe("shared", "key", [](auto sv) {
      if (sv) std::cout << "update: " << *sv << std::endl;
    });

    client.ping(std::string(1024, 'a'))->wait();
    client.set("shared", "key", "value")->wait();
    std::cout << "v: " << *client.get("shared", "key")->wait() << std::endl;

    std::cout << "here" << std::endl;

    for (auto &[k, v] : client.keys("shared")->wait()) { std::cout << "kv: " << v << (int) k << std::endl; }

    client.listen("demo", "event", [](auto data) {
      if (data) std::cout << "event: " << *data << std::endl;
    });

    NotifyToken<int> tok;

    try {
      client.get("registry", "demo")->wait();
    } catch (...) {
      std::cout << "waiting" << std::endl;
      NotifyToken<int> tok;
      client.observe("registry", "demo", [&](auto sv) { tok.notify(0); });
      tok.wait();
    }

    std::cout << "call" << *client.call("demo", "echo", "boom")->wait() << std::endl;

    std::this_thread::sleep_for(10s);
  } catch (std::exception &e) { std::cout << e.what() << std::endl; }
}