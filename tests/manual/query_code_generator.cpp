//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 09.03.17.
//

#include <iostream>

#include "query/backend/data_structures.hpp"
#include "query/backend/cpp/code_generator.hpp"

using std::cout;
using std::endl;

DataStructures basic_data_structures() {
  DataStructures ds;

  ds.GetPropertyIndex("age");
  ds.GetPropertyIndex("name");

  ds.GetLabelIndex("Person");
  ds.GetLabelIndex("Dog");

  ds.GetEdgeTypeIndex("Likes");
  ds.GetEdgeTypeIndex("Hates");

  return ds;
}

DataStructures basic_traversal() {
  DataStructures ds;

  // create a pattern
  auto pattern_ind = ds.AddPattern(DataStructures::Node());
  auto &pattern = ds.patterns()[pattern_ind];
  pattern.nodes_.emplace_back(DataStructures::Node());
  pattern.relationships_.emplace_back(DataStructures::Relationship());

  auto match_ind = ds.AddMatch();
  auto &match = ds.clauses()[match_ind]->As<DataStructures::Match>();
  match.patterns_.emplace_back(pattern_ind);

  return ds;
}

/**
 * For some pre-made data structures generates and prints out code.
 */
int main() {

  std::function<DataStructures()> functions[]{
      basic_data_structures,
      basic_traversal,
  };

  for (auto function : functions) {
    cout << endl << "----------------" << endl;
    cout << query::CodeGenerator(function()).code();
  }
}

