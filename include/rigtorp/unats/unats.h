/*
Copyright (c) 2019 Erik Rigtorp <erik@rigtorp.se>
Copyright 2012-2019 The NATS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#ifndef RIGTORP_UNATS_UNATS_H
#define RIGTORP_UNATS_UNATS_H

#include <array>
#include <cassert>
#include <chrono>
#include <cstdarg>
#include <cstring>
#include <functional>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string_view>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace rigtorp::unats {

enum class Errc {
  NotConnected = ENOTCONN,
  NotEnoughMemory = ENOMEM,
  InvalidArgument = EINVAL,
  WouldBlock = EWOULDBLOCK,
  HostNotFound = 0xFFF + 1,
  System = 0xFFFF + 2,
};

const char *toString(Errc errc) {
  switch (errc) {
  case Errc::NotConnected:
    return "Not connected";
  case Errc::NotEnoughMemory:
    return "Not enough memory";
  case Errc::InvalidArgument:
    return "Invalid argument";
  case Errc::WouldBlock:
    return "Would block";
  case Errc::HostNotFound:
    return "Host not found";
  case Errc::System:
    return "System error";
  }
  return "Unknown error";
}

struct Connection;

struct Options {
  std::string host;
  std::string port;
  std::chrono::milliseconds reconnectInterval = std::chrono::seconds(1);
  std::chrono::milliseconds pingInterval = std::chrono::seconds(1);
  int maxOutstandingPings = 2;
  std::function<void(Connection &)> connectedCb;
  std::function<void(Connection &, std::string_view)> disconnectedCb;
  std::function<void(Connection &, std::string_view, std::string_view)>
      messageCb;
  std::function<void(Connection &)> unblockedCb;
};

struct Connection {

  explicit Connection(const Options &options);

  Connection(Connection &) = delete;

  ~Connection() noexcept;

  Errc subscribe(const char *subject) noexcept;

  // flags can be MSG_MORE to indicate that data should not be sent immediately
  Errc publish(const char *subject, const char *payload, int payloadSize,
               int flags) noexcept;

  // Process messages until timeout
  Errc run(int timeout = -1);

private:
  // Handle async connect
  void handleConnect() noexcept;

  // Read and process messages until the socket would block
  void handleRead() noexcept;

  // Write buffered data until socket would block
  void handleWrite() noexcept;

  __attribute__((format(printf, 2, 3))) void handleError(const char *fmt,
                                                         ...) noexcept;

  // Send control message to server
  __attribute__((format(printf, 2, 3))) Errc sendPrintf(const char *fmt,
                                                        ...) noexcept;

  enum class State { Connecting, Connected, Active, Reconnecting };

  const Options &options_;
  int efd_ = -1;
  int fd_ = -1;
  int sid_ = 1;
  State state_ = State::Reconnecting;
  using time_point = std::chrono::system_clock::time_point;
  time_point deadline_;
  int outstandingPings_ = 0;
  std::vector<char> outBuffer_;
  std::vector<char> inBuffer_;
};

namespace detail {

enum class Op { Info, Msg, Ping, Pong, OK, Err };

struct ParseResult {
  const char *ptr;
  const char *argb[5];
  const char *arge[5];
  int argc;
  Op op;
  Errc ec;
};

inline ParseResult parse(const char *first, const char *last) noexcept {

  enum State {
    OP_START = 0,
    OP_PLUS,
    OP_PLUS_O,
    OP_PLUS_OK,
    OP_MINUS,
    OP_MINUS_E,
    OP_MINUS_ER,
    OP_MINUS_ERR,
    OP_MINUS_ERR_SPC,
    MINUS_ERR_ARG,
    OP_M,
    OP_MS,
    OP_MSG,
    OP_MSG_SPC,
    MSG_ARG,
    MSG_PAYLOAD,
    MSG_END,
    OP_P,
    OP_PI,
    OP_PIN,
    OP_PING,
    OP_PO,
    OP_PON,
    OP_PONG,
    OP_I,
    OP_IN,
    OP_INF,
    OP_INFO,
    OP_INFO_SPC,
    INFO_ARG,
    DONE
  };

  State state = OP_START;
  size_t length = 0;
  ParseResult res = {};

  for (; first != last; ++first) {
    char b = *first;
    switch (state) {
    case OP_START: {
      switch (b) {
      case 'M':
      case 'm':
        state = OP_M;
        break;
      case 'P':
      case 'p':
        state = OP_P;
        break;
      case '+':
        state = OP_PLUS;
        break;
      case '-':
        state = OP_MINUS;
        break;
      case 'I':
      case 'i':
        state = OP_I;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_M: {
      switch (b) {
      case 'S':
      case 's':
        state = OP_MS;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MS: {
      switch (b) {
      case 'G':
      case 'g':
        res.op = Op::Msg;
        state = OP_MSG;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MSG: {
      switch (b) {
      case ' ':
      case '\t':
        state = OP_MSG_SPC;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MSG_SPC: {
      switch (b) {
      case ' ':
      case '\t':
        break;
      default:
        state = MSG_ARG;
        res.argb[res.argc] = first;
        break;
      }
      break;
    }
    case MSG_ARG: {
      switch (b) {
      case ' ':
      case '\t':
        if (res.argc > 3) {
          goto parseErr;
        }
        res.arge[res.argc++] = first;
        state = OP_MSG_SPC;
        break;
      case '\r':
        break;
      case '\n': {
        res.arge[res.argc++] = first;
        length = strtoul(res.argb[res.argc - 1], nullptr, 10);
        state = MSG_PAYLOAD;
        break;
      }
      default:
        break;
      }
      break;
    }
    case MSG_PAYLOAD: {
      if (last - first <= length) {
        res.ec = Errc::WouldBlock;
        goto parseErr;
      }
      res.argb[res.argc] = first;
      res.arge[res.argc] = first + length;
      ++res.argc;
      first += length;
      state = MSG_END;
      break;
    }
    case MSG_END: {
      switch (b) {
      case '\r':
        break;
      case '\n':
        state = DONE;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PLUS: {
      switch (b) {
      case 'O':
      case 'o':
        state = OP_PLUS_O;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PLUS_O: {
      switch (b) {
      case 'K':
      case 'k':
        res.op = Op::OK;
        state = OP_PLUS_OK;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PLUS_OK: {
      switch (b) {
      case '\r':
        break;
      case '\n':
        state = DONE;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MINUS: {
      switch (b) {
      case 'E':
      case 'e':
        state = OP_MINUS_E;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MINUS_E: {
      switch (b) {
      case 'R':
      case 'r':
        state = OP_MINUS_ER;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MINUS_ER: {
      switch (b) {
      case 'R':
      case 'r':
        res.op = Op::Err;
        state = OP_MINUS_ERR;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MINUS_ERR: {
      switch (b) {
      case ' ':
      case '\t':
        state = OP_MINUS_ERR_SPC;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_MINUS_ERR_SPC: {
      switch (b) {
      case ' ':
      case '\t':
        break;
      default:
        state = MINUS_ERR_ARG;
        res.argb[0] = first;
        break;
      }
      break;
    }
    case MINUS_ERR_ARG: {
      switch (b) {
      case '\r':
        res.arge[0] = first;
        break;
      case '\n': {
        if (res.arge[0] == nullptr) {
          res.arge[0] = first;
        }
        state = DONE;
        break;
      }
      }
      break;
    }
    case OP_P: {
      switch (b) {
      case 'I':
      case 'i':
        state = OP_PI;
        break;
      case 'O':
      case 'o':
        state = OP_PO;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PO: {
      switch (b) {
      case 'N':
      case 'n':
        state = OP_PON;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PON: {
      switch (b) {
      case 'G':
      case 'g':
        res.op = Op::Pong;
        state = OP_PONG;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PONG: {
      switch (b) {
      case '\n':
        state = DONE;
        break;
      }
      break;
    }
    case OP_PI: {
      switch (b) {
      case 'N':
      case 'n':
        state = OP_PIN;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PIN: {
      switch (b) {
      case 'G':
      case 'g':
        res.op = Op::Ping;
        state = OP_PING;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_PING: {
      switch (b) {
      case '\n':
        state = DONE;
        break;
      }
      break;
    }
    case OP_I: {
      switch (b) {
      case 'N':
      case 'n':
        state = OP_IN;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_IN: {
      switch (b) {
      case 'F':
      case 'f':
        state = OP_INF;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_INF: {
      switch (b) {
      case 'O':
      case 'o':
        res.op = Op::Info;
        state = OP_INFO;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_INFO: {
      switch (b) {
      case ' ':
      case '\t':
        state = OP_INFO_SPC;
        break;
      default:
        goto parseErr;
      }
      break;
    }
    case OP_INFO_SPC: {
      switch (b) {
      case ' ':
      case 't':
        break;
      default:
        state = INFO_ARG;
        res.argb[0] = first;
        break;
      }
      break;
    }
    case INFO_ARG: {
      switch (b) {
      case '\r':
        res.arge[0] = first;
        break;
      case '\n': {
        if (res.arge[0] == nullptr) {
          res.arge[0] = first;
        }
        state = DONE;
        break;
      }
      default:
        break;
      }
      break;
    }
    case DONE:
      goto done;
      break;
    default:
      goto parseErr;
    }
    assert(first <= last);
  }

done:

  if (state != DONE) {
    res.ec = Errc::WouldBlock;
  }

  res.ptr = first;

  return res;

parseErr:
  res.ptr = first;
  if (res.ec == Errc{}) {
    res.ec = Errc::InvalidArgument;
  }
  return res;
}

struct XRecvResult {
  size_t received;
  Errc ec;
};

XRecvResult xrecv(int fd, char *buf, size_t len) {
  XRecvResult res = {};
  for (;;) {
    ssize_t recieved = recv(fd, buf, len, 0);
    if (recieved == 0) {
      res.ec = Errc::NotConnected;
      break;
    }
    if (recieved == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        res.ec = Errc::WouldBlock;
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == ECONNREFUSED || errno == ENOTCONN) {
        res.ec = Errc::NotConnected;
        break;
      }
      if (errno == EINVAL) {
        res.ec = Errc::InvalidArgument;
        break;
      }
      if (errno == ENOMEM) {
        res.ec = Errc::NotEnoughMemory;
        break;
      }
      res.ec = Errc::System;
      break;
    }
    res.received = recieved;
    break;
  }
  return res;
}

struct XSendResult {
  size_t sent;
  Errc ec;
};

// Send data until socket would block or an error occurs
XSendResult xsend(int fd, const char *buf, size_t len, int flags) {
  XSendResult res = {};
  const char *first = buf;
  const char *last = first + len;
  for (; first != last;) {
    const ssize_t sent = send(fd, first, last - first, MSG_NOSIGNAL | flags);
    if (sent == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        res.ec = Errc::WouldBlock;
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == ECONNRESET || errno == ENOTCONN || errno == EPIPE) {
        res.ec = Errc::NotConnected;
        break;
      }
      if (errno == EINVAL) {
        res.ec = Errc::InvalidArgument;
        break;
      }
      if (errno == ENOMEM) {
        res.ec = Errc::NotEnoughMemory;
        break;
      }
      res.ec = Errc::System;
      break;
    }
    first += sent;
  }
  res.sent = first - buf;
  return res;
}

// Send data until socket would block or an error occurs
// Each iovec will be updated to indicate remaining data to be sent
XSendResult xsendmsg(int fd, struct iovec *iov, int iovcnt, int flags) {
  XSendResult res = {};
  msghdr msg = {};
  ssize_t sent = 0;
  for (;;) {
    msg.msg_iov = iov;
    msg.msg_iovlen = iovcnt;
    ssize_t rc = sendmsg(fd, &msg, MSG_NOSIGNAL | flags);
    if (rc == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        res.ec = Errc::WouldBlock;
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == ECONNRESET || errno == ENOTCONN || errno == EPIPE) {
        res.ec = Errc::NotConnected;
        break;
      }
      if (errno == EINVAL) {
        res.ec = Errc::InvalidArgument;
        break;
      }
      if (errno == ENOMEM) {
        res.ec = Errc::NotEnoughMemory;
        break;
      }
      res.ec = Errc::System;
      break;
    }
    sent += rc;

    int niov = 0;
    for (; niov < iovcnt && rc > iov[niov].iov_len; ++niov) {
      rc -= iov[niov].iov_len;
      iov[niov].iov_len = 0;
    }
    iov += niov;
    iovcnt -= niov;
    iov[0].iov_base += rc;
    iov[0].iov_len -= rc;
    if (iov[0].iov_len == 0) {
      break;
    }
  }
  res.sent = sent;
  return res;
}

struct XconnectResult {
  int fd;
  Errc ec;
};

// Synchronously resolve host and asynchronously connect to it
XconnectResult xconnect(const char *host, const char *port) noexcept {

  XconnectResult res = {-1, Errc{}};

  addrinfo hints = {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = 0;
  hints.ai_protocol = 0;

  addrinfo *result = nullptr, *rp = nullptr;

  if (int rc = getaddrinfo(host, port, &hints, &result); rc != 0) {
    if (rc == EAI_BADFLAGS || rc == EAI_SOCKTYPE) {
      res.ec = Errc::InvalidArgument;
      return res;
    }
    if (rc == EAI_MEMORY) {
      res.ec = Errc::NotEnoughMemory;
      return res;
    }
    if (rc == EAI_SYSTEM) {
      if (errno == ENOMEM) {
        res.ec = Errc::NotEnoughMemory;
        return res;
      }
      res.ec = Errc::System;
      return res;
    }
    res.ec = Errc::HostNotFound;
    return res;
  }

  int fd = -1;
  for (rp = result; rp != nullptr; rp = rp->ai_next) {
    fd = socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                rp->ai_protocol);
    if (fd == -1) {
      continue; // Try next address
    }

    if (connect(fd, rp->ai_addr, rp->ai_addrlen) != -1) {
      break; // Success
    }

    if (errno == EINPROGRESS) {
      break; // Success
    }

    close(fd); // Try next address
  }

  if (rp == nullptr) {
    // No address succeeded
    res.ec = Errc::System;
  } else {
    res.fd = fd;
  }

  freeaddrinfo(result);

  return res;
}

} // namespace detail

Connection::Connection(const Options &options) : options_(options) {
  efd_ = epoll_create1(EPOLL_CLOEXEC);
  outBuffer_.reserve(10000);
}

Connection::~Connection() noexcept {
  close(fd_);
  close(efd_);
}

Errc Connection::subscribe(const char *subject) noexcept {
  if (state_ != State::Active) {
    return Errc::NotConnected;
  }

  return sendPrintf("SUB %s %i\r\n", subject, ++sid_);
}

Errc Connection::publish(const char *subject, const char *payload,
                         int payloadSize, int flags) noexcept {
  if (payloadSize < 0) {
    return Errc::InvalidArgument;
  }

  if (flags & (~MSG_MORE)) {
    return Errc::InvalidArgument;
  }

  if (state_ != State::Active) {
    return Errc::NotConnected;
  }

  char command[512];

  if (sizeof(command) + payloadSize + 2 > outBuffer_.capacity()) {
    return Errc::NotEnoughMemory;
  }

  if (sizeof(command) + payloadSize + 2 >
      outBuffer_.capacity() - outBuffer_.size()) {
    return Errc::WouldBlock;
  }

  int size =
      snprintf(command, sizeof(command), "PUB %s %i\r\n", subject, payloadSize);
  if (size == -1) {
    return Errc::NotEnoughMemory;
  }
  if (size > sizeof(command)) {
    return Errc::NotEnoughMemory;
  }

  iovec iov[3];
  iov[0].iov_base = command;
  iov[0].iov_len = size;
  iov[1].iov_base = (void *)payload;
  iov[1].iov_len = payloadSize;
  iov[2].iov_base = (void *)"\r\n";
  iov[2].iov_len = 2;

  Errc res = {};
  if (outBuffer_.empty()) {
    // Fast path, socket is writable; immediately send message
    res = detail::xsendmsg(fd_, iov, 3, flags).ec;
  }

  // Copy unsent data to buffer for sending when socket becomes writable
  for (int i = 0; i < 3; ++i) {
    outBuffer_.insert(outBuffer_.end(), (char *)iov[i].iov_base,
                      (char *)iov[i].iov_base + iov[i].iov_len);
  }

  return res;
}

void Connection::handleConnect() noexcept {
  int err = 0;
  socklen_t errLen = sizeof(err);
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &errLen) == -1) {
    handleError("getsockopt: %s", strerror(errno));
    return;
  }
  if (err != 0) {
    handleError("connect: %s", strerror(err));
    return;
  }
  int one = 1;
  if (setsockopt(fd_, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) == -1) {
    handleError("setsockopt: %s", strerror(errno));
  }
  state_ = State::Connected;
  inBuffer_.clear();
  outBuffer_.clear();
}

// Read and process messages until the socket would block
void Connection::handleRead() noexcept {
  for (;;) {
    const auto oldSize = inBuffer_.size();
    inBuffer_.resize(32000);
    auto res = detail::xrecv(fd_, inBuffer_.data() + oldSize,
                             inBuffer_.size() - oldSize);
    inBuffer_.resize(oldSize + res.received);
    if (res.ec != Errc{}) {
      if (res.ec == Errc::WouldBlock) {
        break;
      }
      handleError("recv: %s", toString(res.ec));
      return;
    }
    const char *first = inBuffer_.data();
    const char *last = inBuffer_.data() + inBuffer_.size();
    for (; first != last;) {
      const auto res = detail::parse(first, last);
      if (res.ec != Errc{}) {
        if (res.ec == Errc::WouldBlock) {
          break;
        }
        handleError("parse error at '%c'", *res.ptr);
        return;
      }

      switch (res.op) {
      case detail::Op::Info: {
        if (auto rc = sendPrintf(
                "CONNECT "
                "{\"verbose\":false,\"pedantic\":false,\"tls_required\":"
                "false,\"name\":\"test\",\"lang\":\"c++\",\"version\":\"0.0."
                "0\",\"protocol\":1}\r\n");
            rc != Errc{}) {
          handleError("send: %s", toString(rc));
        }
        state_ = State::Active;
        deadline_ = std::chrono::system_clock::now() + options_.pingInterval;
        if (options_.connectedCb) {
          options_.connectedCb(*this);
        }
        break;
      }
      case detail::Op::Msg: {
        if (options_.messageCb) {
          options_.messageCb(
              *this, std::string_view(res.argb[0], res.arge[0] - res.argb[0]),
              std::string_view(res.argb[res.argc - 1],
                               res.arge[res.argc - 1] -
                                   res.argb[res.argc - 1]));
        }
        break;
      }
      case detail::Op::Ping: {
        if (auto rc = sendPrintf("PONG\r\n"); rc != Errc{}) {
          handleError("send: %s", toString(rc));
        }
        break;
      }
      case detail::Op::Pong:
        outstandingPings_ = std::max(outstandingPings_ - 1, 0);
        break;
      case detail::Op::OK:
        break;
      case detail::Op::Err:
        handleError("%.*s", (int)(res.arge[0] - res.argb[0]), res.argb[0]);
        break;
      }

      first = res.ptr;
    }
    assert(first <= inBuffer_.data() + inBuffer_.size());
    inBuffer_.erase(inBuffer_.begin(),
                    inBuffer_.begin() + (first - inBuffer_.data()));
    assert(inBuffer_.size() == last - first);
  }
}

__attribute__((format(printf, 2, 3))) Errc
Connection::sendPrintf(const char *fmt, ...) noexcept {
  char command[512];

  if (sizeof(command) > outBuffer_.capacity() - outBuffer_.size()) {
    return Errc::WouldBlock;
  }

  va_list ap;
  va_start(ap, fmt);
  int size = vsnprintf(command, sizeof(command), fmt, ap);
  va_end(ap);

  if (size == -1) {
    return Errc::InvalidArgument;
  }
  if (size > sizeof(command)) {
    return Errc::NotEnoughMemory;
  }

  detail::XSendResult res = {};
  if (outBuffer_.empty()) {
    res = detail::xsend(fd_, command, size, 0);
  }

  outBuffer_.insert(outBuffer_.end(), command + res.sent, command + size);

  return res.ec;
}

// Write buffered data until socket would block
void Connection::handleWrite() noexcept {
  const auto res = detail::xsend(fd_, outBuffer_.data(), outBuffer_.size(), 0);
  outBuffer_.erase(outBuffer_.begin(), outBuffer_.begin() + res.sent);
  if (res.ec != Errc{}) {
    handleError("send: %s", toString(res.ec));
    return;
  }
  if (outBuffer_.empty() && options_.unblockedCb) {
    options_.unblockedCb(*this);
  }
}

__attribute__((format(printf, 2, 3))) void
Connection::handleError(const char *fmt, ...) noexcept {
  std::array<char, 1024> buf = {};
  va_list ap;
  va_start(ap, fmt);
  int size = vsnprintf(buf.data(), buf.size(), fmt, ap);
  va_end(ap);

  close(fd_);
  fd_ = -1;
  state_ = State::Reconnecting;
  outstandingPings_ = 0;

  if (options_.disconnectedCb) {
    options_.disconnectedCb(*this, std::string_view(buf.data(), size));
  }
}

Errc Connection::run(int timeout) {

  const auto userDeadline = timeout == -1
                                ? std::chrono::system_clock::time_point::max()
                                : std::chrono::system_clock::now() +
                                      std::chrono::milliseconds(timeout);

  std::array<epoll_event, 2> events = {};

  auto now = std::chrono::system_clock::now();
  for (;;) {
    const auto nextDeadline = std::max(std::min(userDeadline, deadline_), now);
    const int epollTimeout =
        std::chrono::duration_cast<std::chrono::milliseconds>(nextDeadline -
                                                              now)
            .count();
    int nfds = epoll_wait(efd_, events.data(), events.size(), epollTimeout);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      return Errc::System;
    }
    for (int i = 0; i < nfds; ++i) {
      if (events[i].data.fd == fd_) {
        switch (state_) {
        case State::Connecting:
          handleConnect();
          break;
        case State::Connected:
          if (events[i].events & EPOLLIN) {
            handleRead();
          }
          break;
        case State::Active:
          if (events[i].events & EPOLLIN) {
            handleRead();
          }
          if (events[i].events & EPOLLOUT) {
            handleWrite();
          }
          break;
        case State::Reconnecting:
          break;
        }
      }
    }
    now = std::chrono::system_clock::now();
    if (now > deadline_) {
      switch (state_) {
      case State::Reconnecting: {
        deadline_ = now + options_.reconnectInterval;
        auto res =
            detail::xconnect(options_.host.c_str(), options_.port.c_str());
        if (res.ec != Errc{}) {
          handleError("connect: %s", toString(res.ec));
        } else {
          fd_ = res.fd;
          epoll_event ev = {};
          ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
          ev.data.fd = fd_;
          if (epoll_ctl(efd_, EPOLL_CTL_ADD, fd_, &ev) == -1) {
            return Errc::System;
          }
          state_ = State::Connecting;
        }
        break;
      }
      case State::Connecting:
      case State::Connected:
        handleError("connect: %s", strerror(ETIMEDOUT));
        deadline_ += options_.reconnectInterval;
        break;
      case State::Active:
        if (outstandingPings_ > options_.maxOutstandingPings) {
          handleError("ping: %s", strerror(ETIMEDOUT));
          deadline_ += options_.reconnectInterval;
        } else {
          if (auto rc = sendPrintf("PING\r\n"); rc != Errc{}) {
            handleError("send: %s", toString(rc));
          }
          outstandingPings_++;
          deadline_ += options_.pingInterval;
        }
        break;
      }
    }
    if (now > userDeadline) {
      break;
    }
  }
  return Errc{};
}

} // namespace rigtorp::unats

#endif // RIGTORP_UNATS_UNATS_H
