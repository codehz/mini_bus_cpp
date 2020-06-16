#include <sstream>
#include <variant>
#include <optional>
#include <string>
#include <string_view>
#include <condition_variable>
#include <map>
#include <random>
#include <thread>
#include <mutex>
#include <boost/asio.hpp>

namespace mini_bus {
namespace details {

constexpr int repr_impl(char const *str, int step = 4, int val = 0) {
  return step == 0 ? val : repr_impl(str + 1, step - 1, val | (*str << ((4 - step) * 8)));
}

constexpr int repr(char const *str) { return repr_impl(str); }

template <typename Stream> inline size_t read_exactly(Stream &stream, char *buf, size_t len) {
  return read(stream, buffer(buf, len), transfer_all());
}

template <typename Stream, size_t size> inline size_t read_exactly(Stream &stream, char (&buf)[size]) {
  return read_exactly(stream, buf, size);
}

} // namespace details

using namespace boost::asio;

struct IBaseNotifyToken {
  inline virtual ~IBaseNotifyToken() {}
  virtual void failed(std::exception_ptr ptr) = 0;
};

template <typename T> struct INotifyToken : virtual IBaseNotifyToken { virtual void notify(T &&) = 0; };

template <typename T> class NotifyToken : public INotifyToken<T> {
protected:
  std::mutex mtx;
  std::condition_variable cv;
  std::optional<T> value;
  std::exception_ptr ex;

public:
  inline void reset() {
    std::lock_guard lock{mtx};
    value.reset();
    ex = nullptr;
  }
  inline T wait() {
    std::unique_lock lock{mtx};
    cv.wait(lock, [this] { return ex || value.has_value(); });
    if (ex) std::rethrow_exception(ex);
    return *value;
  }
  inline void notify(T &&rhs) override {
    {
      std::lock_guard lock{mtx};
      value.emplace(std::move(rhs));
    }
    cv.notify_one();
  }
  inline void failed(std::exception_ptr ptr) override {
    {
      std::lock_guard lock{mtx};
      ex = ptr;
    }
    cv.notify_one();
  }
};

template <typename T> class SyncNotifyToken : public NotifyToken<T> {
public:
  inline void notify(T &&rhs) override {
    NotifyToken<T>::notify(std::move(rhs));
    std::unique_lock lock{this->mtx};
    this->cv.wait(lock, [this] { return !this->value.has_value(); });
  }

  inline void notifySource() {
    this->reset();
    this->cv.notify_one();
  }
};

class MiniBusEncoder {
  std::string buffer;

public:
  inline std::string_view view() const { return buffer; }

  inline void insert_short_string(std::string_view str) {
    buffer += (unsigned char) str.length();
    buffer += str;
  }

  inline void insert_varuint(uint64_t vuit) {
    buffer.reserve(buffer.size() + vuit + 16);
    while (true) {
      if (vuit < 128) {
        buffer += (unsigned char) vuit;
        break;
      }
      buffer += (unsigned char) (0b1000000 | (vuit & 0b01111111));
      vuit >>= 7;
    }
  }

  inline void insert_long_string(std::string_view str) {
    insert_varuint(str.length());
    buffer += str;
  }

  inline void insert_rid(uint32_t rid) { buffer.append((char *) &rid, sizeof rid); }
};

class MiniBusException : public std::runtime_error {
public:
  inline MiniBusException(std::string data) : runtime_error(data) {}
};

class MiniBusPacket {
  bool is_ok;
  std::optional<std::string> m_payload;

public:
  inline MiniBusPacket(bool is_ok, std::optional<std::string> m_payload = {}) : is_ok(is_ok), m_payload(m_payload) {}

  inline bool ok() const noexcept { return is_ok; }
  inline bool has_payload() const noexcept { return m_payload.has_value(); }
  inline std::optional<std::string> const &payload() const { return m_payload; }
  inline std::optional<std::string> &payload() { return m_payload; }
};

template <typename Stream> class MiniBusPacketDecoder {
  Stream &stream;

  inline uint64_t read_varuint() {
    char size[1];
    details::read_exactly(stream, size);
    if (size[0] < 128) return size[0];
    return (((int64_t) size[0] | 0b01111111) << 7) + read_varuint();
  }

  inline std::string read_short_binary() {
    char sizearr[1];
    details::read_exactly(stream, sizearr);
    std::string buf;
    buf.resize(sizearr[0], 0);
    details::read_exactly(stream, buf.data(), sizearr[0]);
    return buf;
  }

  inline std::string read_long_binary() {
    auto size = read_varuint();
    std::string buf;
    buf.resize(size, 0);
    details::read_exactly(stream, buf.data(), size);
    return buf;
  }

public:
  inline MiniBusPacketDecoder(Stream &stream) : stream(stream) {}

  inline std::string decode_payload() { return read_long_binary(); }

  inline MiniBusPacket decode_body(unsigned char flag) {
    switch (flag) {
    case 0: return {true};
    case 1: return {true, decode_payload()};
    case 255: return {false, decode_payload()};
    }
    throw std::runtime_error{"Unknown packet"};
  }

  struct data {
    uint32_t rid;
    uint32_t type;
    MiniBusPacket pkt;
  };

  inline data decode() {
    union {
      char buffer[9];
      struct {
        uint32_t rid;
        uint32_t type;
        unsigned char flag;
      };
    } u;
    details::read_exactly(stream, u.buffer);
    auto body = decode_body(u.flag);
    return {u.rid, u.type, std::move(body)};
  }
};

class MiniBusClient {
  std::random_device rd;
  std::uniform_int_distribution<uint32_t> dist;
  std::mutex mtx;
  std::map<uint64_t, std::shared_ptr<NotifyToken<std::optional<std::string>>>> reqmap;
  std::map<uint64_t, std::function<void(std::string_view)>> evtmap;
  std::map<std::string, std::function<std::string_view(std::string_view)>, std::less<>> fnmap;
  ip::tcp::socket socket;
  std::unique_ptr<std::thread> work_thread;

  inline uint32_t select_rid() {
    while (true) {
      uint32_t ret = dist(rd);
      if (reqmap.count(ret) != 0) continue;
      return ret;
    }
  }

  inline std::shared_ptr<NotifyToken<std::optional<std::string>>>
  send_simple(std::string_view command, std::string_view payload = {}) {
    std::unique_lock lock{mtx};
    auto rid = select_rid();
    MiniBusEncoder encoder;
    encoder.insert_rid(rid);
    encoder.insert_short_string(command);
    encoder.insert_long_string(payload);
    auto view = encoder.view();
    write(socket, buffer(view), transfer_all());
    auto tok = std::make_shared<NotifyToken<std::optional<std::string>>>();
    reqmap.emplace(rid, tok);
    return tok;
  }

  inline std::shared_ptr<SyncNotifyToken<std::optional<std::string>>>
  send_event(std::string_view command, std::string_view payload, uint32_t &rid) {
    std::unique_lock lock{mtx};
    rid = select_rid();
    MiniBusEncoder encoder;
    encoder.insert_rid(rid);
    encoder.insert_short_string(command);
    encoder.insert_long_string(payload);
    auto view = encoder.view();
    write(socket, buffer(view), transfer_all());
    auto tok = std::make_shared<SyncNotifyToken<std::optional<std::string>>>();
    reqmap.emplace(rid, tok);
    return tok;
  }

  inline void send_response(uint32_t rid, std::string_view command, std::string_view payload) {
    MiniBusEncoder encoder;
    encoder.insert_rid(rid);
    encoder.insert_short_string(command);
    encoder.insert_long_string(payload);
    auto view = encoder.view();
    write(socket, buffer(view), transfer_all());
  }

  inline void worker() {
    try {
      MiniBusPacketDecoder decoder{socket};
      while (true) {
        auto data = decoder.decode();
        std::lock_guard lock{mtx};
        switch (data.type) {
        case details::repr("RESP"): {
          auto it = reqmap.find(data.rid);
          if (it == reqmap.end()) continue;
          auto [k, v] = *it;
          reqmap.erase(it);
          if (data.pkt.ok()) {
            v->notify(std::move(data.pkt.payload()));
          } else {
            v->failed(std::make_exception_ptr(MiniBusException{*data.pkt.payload()}));
          }
        } break;
        case details::repr("NEXT"): {
          if (data.pkt.ok()) {
            evtmap[data.rid](*data.pkt.payload());
          } else {
            evtmap.erase(data.rid);
          }
        } break;
        case details::repr("CALL"): {
          std::string_view sv = *data.pkt.payload();
          if (sv.length() < 1) throw std::runtime_error("Unexcepted call");
          auto len = sv[0];
          sv.remove_prefix(1);
          if (sv.length() < len) throw std::runtime_error("Unexcepted call");
          auto it = fnmap.find(sv.substr(0, len));
          if (it == fnmap.end()) {
            send_response(data.rid, "EXCEPTION", "Not found");
            continue;
          }
          sv.remove_prefix(len);
          try {
            auto ret = it->second(sv);
            send_response(data.rid, "RESPONSE", ret);
          } catch (std::runtime_error const &e) { send_response(data.rid, "EXCEPTION", e.what()); }
        } break;
        }
      }
    } catch (...) {}

    for (auto &[k, v] : reqmap) v->failed(std::make_exception_ptr(std::runtime_error{"closed"}));
  }

public:
  enum class ACL {
    Private,
    Protected,
    Public,
  };

  inline MiniBusClient(io_service &io, ip::address address, unsigned short port) : socket(io) {
    socket.connect(ip::tcp::endpoint{address, port});

    write(socket, buffer("MINIBUS"), transfer_all());
    char ok[2];
    details::read_exactly(socket, ok);
    if (memcmp(&ok[0], "OK", 2) != 0) throw std::runtime_error{"ProtocolError"};

    work_thread = std::make_unique<std::thread>([this] { worker(); });
  }

  inline ~MiniBusClient() {
    boost::system::error_code ec;
    socket.close(ec);
    if (work_thread && work_thread->joinable()) work_thread->join();
  }

  inline void register_handler(std::string const &name, std::function<std::string_view(std::string_view)> fn) {
    fnmap.emplace(name, fn);
  }

  inline std::string ping(std::string_view payload = {}) { return *send_simple("PING", payload)->wait(); }

  inline void stop() { send_simple("STOP")->wait(); }

  inline void set_private(std::string_view key, std::string_view value) {
    std::ostringstream oss;
    oss << (unsigned char) key.length() << key;
    oss << value;
    auto buf = oss.str();
    send_simple("SET PRIVATE", buf)->wait();
  }

  inline std::string get_private(std::string_view key) {
    std::ostringstream oss;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    return *send_simple("GET PRIVATE", buf)->wait();
  }

  inline void del_private(std::string_view key) {
    std::ostringstream oss;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    send_simple("DEL PRIVATE", buf)->wait();
  }

  inline void acl(std::string_view key, ACL acl) {
    std::ostringstream oss;
    oss << (unsigned char) key.length() << key;
    switch (acl) {
    case ACL::Private: oss << "private"; break;
    case ACL::Protected: oss << "protected"; break;
    case ACL::Public: oss << "public"; break;
    default: throw std::invalid_argument("Invalid ACL");
    }
    auto buf = oss.str();
    send_simple("DEL PRIVATE", buf)->wait();
  }

  inline void notify(std::string_view key, std::string_view value) {
    std::ostringstream oss;
    oss << (unsigned char) key.length() << key;
    oss << value;
    auto buf = oss.str();
    send_simple("NOTIFY", buf)->wait();
  }

  inline void set(std::string_view bucket, std::string_view key, std::string_view value) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    oss << value;
    auto buf = oss.str();
    send_simple("SET", buf)->wait();
  }

  inline std::string get(std::string_view bucket, std::string_view key) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    return *send_simple("GET", buf)->wait();
  }

  inline void del(std::string_view bucket, std::string_view key) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    send_simple("DEL", buf)->wait();
  }

  inline std::list<std::tuple<ACL, std::string>> keys(std::string_view bucket) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    auto buf = oss.str();
    auto res = *send_simple("KEYS", buf)->wait();
    std::istringstream iss{res};
    std::list<std::tuple<ACL, std::string>> ret;
    while (true) {
      ACL acl;
      unsigned char len;
      iss >> len;
      if (!iss) break;
      char buf[16] = {};
      iss.read(buf, len);
      if (strcmp(buf, "private") == 0)
        acl = ACL::Private;
      else if (strcmp(buf, "protected") == 0)
        acl = ACL::Protected;
      else if (strcmp(buf, "public") == 0)
        acl = ACL::Public;
      iss >> len;
      char key[256];
      iss.read(key, len);
      ret.emplace_back(acl, std::string{key, (size_t) len});
    }
    return ret;
  }

  inline std::string call(std::string_view bucket, std::string_view key, std::string_view value) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    oss << value;
    auto buf = oss.str();
    return *send_simple("CALL", buf)->wait();
  }

  inline void observe(std::string_view bucket, std::string_view key, std::function<void(std::string_view)> cb) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    uint32_t rid;
    auto tok = send_event("OBSERVE", buf, rid);
    tok->wait();
    evtmap.emplace(rid, cb);
    tok->notifySource();
  }

  inline void listen(std::string_view bucket, std::string_view key, std::function<void(std::string_view)> cb) {
    std::ostringstream oss;
    oss << (unsigned char) bucket.length() << bucket;
    oss << (unsigned char) key.length() << key;
    auto buf = oss.str();
    uint32_t rid;
    auto tok = send_event("LISTEN", buf, rid);
    tok->wait();
    evtmap.emplace(rid, cb);
    tok->notifySource();
  }

  inline void join() { work_thread->join(); }
};

} // namespace mini_bus