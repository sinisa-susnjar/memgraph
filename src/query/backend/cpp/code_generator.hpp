#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>

#include "database/graph_db_accessor.hpp"
#include "query/backend/data_structures.hpp"

namespace query {


class CodeGenerator {
public:

  CodeGenerator(const DataStructures &data_structures) :
  data_structures_(data_structures) {
    Generate();
  }

  const std::string &code() {
    return code_;
  }

private:
  const DataStructures &data_structures_;
  std::string code_;

  /*
   * Helper functions for emitting code. All of them
   * emit some code and return a reference to this,
   * for chaining.
   */

  /** Emits a newline. */
  CodeGenerator &NL();
  /** Emits the given string and a newline */
  CodeGenerator &NL(const std::string &s);

  /**
   * Adds a tab to the generated code and returns
   * a reference to this.
   */
  CodeGenerator &Tab();


  /**
   * Adds a comment to the generated code and returns
   * a reference to this.
   */
  CodeGenerator &Comm(const std::string &comment);


  /** Emits a string */
  CodeGenerator &Emit(const std::string &s);

  /** Emits a string */
  CodeGenerator &Emit(const char *s);

  /** Emits a number convertible with std::to_string */
  template <typename TArg>
  CodeGenerator &Emit(const TArg num) { return Emit(std::to_string(num)); }

  /** Emits all the given arguments. */
  template <typename TFirst, typename ... TOthers>
  CodeGenerator &Emit(const TFirst &first, const TOthers &... others) {
    return Emit(first).Emit(others ...);
  };

  /** Formats and emits a string */
  template <typename ... TArgs>
  CodeGenerator Fmt(const std::string &format, const TArgs ... args) {
    code_ += fmt::format(format, args...);
    return *this;
  }

  /*
   * Functions that convert the data structures
   * into emitted code.
   */

  void Generate();
  void GenerateNamedStuff();
  void GenerateTraversal();
  void GeneratePatternTraversal(const DataStructures::Pattern &pattern,
                                int variable_index);
  void GenerateReturn();
};
}

