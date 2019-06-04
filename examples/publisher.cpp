/*
Copyright (c) 2019 Erik Rigtorp <erik@rigtorp.se>

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

#include <iostream>
#include <rigtorp/unats/unats.h>

int main(int argc, char *argv[]) {

  using namespace rigtorp::unats;

  bool connected = false;
  bool blocked = false;

  Options options;
  options.host = "127.0.0.1";
  options.port = "4222";
  options.connectedCb = [&](Connection &c) {
    std::cout << "connected" << std::endl;
    connected = true;
  };
  options.disconnectedCb = [&](Connection &, auto error) {
    std::cerr << "disconnected: " << error << std::endl;
    connected = false;
  };
  options.unblockedCb = [&](Connection &) {
    // can start producing work again
    std::cout << "unblocked" << std::endl;
    blocked = false;
  };

  Connection c(options);

  while (!connected) {
    if (auto rc = c.run(100); rc != Errc{}) {
      std::cout << "error " << toString(rc) << std::endl;
      return 1;
    }
  }

  char buf[9000];
  memset(buf, 'X', sizeof(buf));

  for (int i = 0; i < 10000000 && connected; ++i) {
    while (blocked && connected) {
      if (auto rc = c.run(100); rc != Errc{}) {
        std::cout << "error " << toString(rc) << std::endl;
        return 1;
      }
    }
    c.run(0);
    if (auto rc = c.publish("subject", buf, sizeof(buf), 0); rc != Errc{}) {
      if (rc == Errc::WouldBlock) {
        // nats-server or network is giving backpressure; stop producing for
        // until backlog clears
        blocked = true;
        std::cout << "blocked" << std::endl;
        continue;
      }
      std::cout << "error " << toString(rc) << std::endl;
      return 1;
    }
  }

  return 0;
}
