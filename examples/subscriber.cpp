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

  Options options;
  options.host = "127.0.0.1";
  options.port = "4222";
  options.connectedCb = [](Connection &c) {
    std::cout << "connected" << std::endl;
    auto rc = c.subscribe("subject");
    if (rc != Errc{}) {
      std::cout << "subscribe error " << toString(rc) << std::endl;
    }
  };
  options.disconnectedCb = [](Connection &, auto error) {
    std::cerr << "disconnected: " << error << std::endl;
  };
  options.messageCb = [](Connection &c, auto sub, auto msg) {
    std::cout << "received message: " << sub << " " << msg << std::endl;
  };

  Connection c(options);

  c.run();

  return 0;
}
