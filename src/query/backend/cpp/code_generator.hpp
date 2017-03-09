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

  void Generate();
  void GenerateNamedStuff();
  void GenerateTraversal();
  void GeneratePatternTraversal(const DataStructures::Pattern &pattern,
                                int variable_index);
};
}

