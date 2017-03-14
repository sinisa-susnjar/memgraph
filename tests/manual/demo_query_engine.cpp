#include <iostream>

#include "dbms/dbms.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.cpp"
#include "query/engine.hpp"

using std::cout;
using std::cin;
using std::endl;

class ConsoleResultStream : public Loggable {
 public:
  ConsoleResultStream() : Loggable("ConsoleResultStream") {}

  void Header(const std::vector<std::string>&) { logger.info("header"); }

  void Result(std::vector<TypedValue>& values) {
    for (auto value : values) {
      logger.info("    result");
    }
  }

  void Summary(const std::map<std::string, TypedValue>&) {
    logger.info("summary");
  }
};

int main(int argc, char* argv[]) {
  // init arguments
  REGISTER_ARGS(argc, argv);

  // init logger
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());

  // init db context
  Dbms dbms;
  ConsoleResultStream stream;
  QueryEngine<ConsoleResultStream> query_engine;

  // TODO: inject some data

  cout << "-- Memgraph Query Engine --" << endl;
  while (true) {
    // read command
    cout << "> ";
    std::string command;
    std::getline(cin, command);
    if (command == "quit") break;
    // execute command / query
    try {
      auto db_accessor = dbms.active();
      query_engine.Run(command, db_accessor, stream);
    } catch (const std::exception& e) {
      cout << e.what() << endl;
    } catch (const QueryEngineException& e) {
      cout << e.what() << endl;
    }
  }

  return 0;
}
