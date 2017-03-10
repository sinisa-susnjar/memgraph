#pragma once

#include <string>
#include <vector>

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
   * Helper functions for emitting code.
   */

  /**
   * Adds a newline to the generated code and returns
   * a reference to this.
   */
  CodeGenerator &NL();

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

