//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#include <memory>

#include "gtest/gtest.h"

#include "query/data_structures.hpp"


TEST(QueryDataStructures, Variables) {
  DataStructures ds;

  auto var1 = ds.GetVariableIndex("var1");
  auto var2 = ds.GetVariableIndex("var2");

  EXPECT_NE(var1, var2);
  EXPECT_EQ(var1, ds.GetVariableIndex("var1"));
  EXPECT_EQ(ds.Variables()[var1], "var1");
}

TEST(QueryDataStructures, Properties) {
  DataStructures ds;

  auto prop1 = ds.GetPropertyIndex("prop1");
  auto prop2 = ds.GetPropertyIndex("prop2");

  EXPECT_NE(prop1, prop2);
  EXPECT_EQ(prop1, ds.GetPropertyIndex("prop1"));
  EXPECT_EQ(ds.Properties()[prop1], "prop1");
}

TEST(QueryDataStructures, Labels) {
  DataStructures ds;

  auto label1 = ds.GetLabelIndex("label1");
  auto label2 = ds.GetLabelIndex("label2");

  EXPECT_NE(label1, label2);
  EXPECT_EQ(label1, ds.GetLabelIndex("label1"));
  EXPECT_EQ(ds.Labels()[label1], "label1");
}

TEST(QueryDataStructures, EdgeTypes) {
  DataStructures ds;

  auto edge_type1 = ds.GetEdgeTypeIndex("edge_type1");
  auto edge_type2 = ds.GetEdgeTypeIndex("edge_type2");

  EXPECT_NE(edge_type1, edge_type2);
  EXPECT_EQ(edge_type1, ds.GetEdgeTypeIndex("edge_type1"));
  EXPECT_EQ(ds.EdgeTypes()[edge_type1], "edge_type1");
}

TEST(QueryDataStructures, Expression) {
  DataStructures ds;

  auto expr1_ind = ds.AddExpression(DataStructures::ExpressionOp::Addition);
  auto &expr1 = ds.Expressions()[expr1_ind];
  EXPECT_EQ(expr1.op_, DataStructures::ExpressionOp::Addition);
  EXPECT_EQ(ds.Expressions().size(), 1);

  auto &expr2 = ds.Expressions()[ds.AddExpression(DataStructures::ExpressionOp::Subtraction)];
  expr2.operands_.emplace_back(DataStructures::ExpressionOperand ::Expression, 42);
  EXPECT_EQ(expr2.operands_[0].first, DataStructures::ExpressionOperand::Expression);
  EXPECT_EQ(expr2.operands_[0].second, 42);
}

TEST(QueryDataStructures, Pattern) {
  DataStructures ds;

  auto start_node = DataStructures::Node();
  EXPECT_EQ(ds.Patterns().size(), 0);
  ds.AddPattern(start_node);
  EXPECT_EQ(ds.Patterns().size(), 1);
}

TEST(QueryDataStructures, MatchClause) {
  DataStructures ds;

  EXPECT_EQ(ds.Clauses().size(), 0);
  auto clause_ind = ds.AddMatch();
  EXPECT_EQ(ds.Clauses().size(), 1);
  EXPECT_EQ(-1, ds.Clauses()[clause_ind]->As<DataStructures::Match>()->expression_);
}

//TEST(QueryDataStructures, Clause) {
//  DataStructures ds;
//
//  EXPECT_EQ(ds.Clauses().size(), 0);
//  auto &clause1 = ds.AddClause(DataStructures::ClauseType::Merge);
//  clause1.elements_.push_back(42);
//  clause1.elements_.push_back(17);
//  EXPECT_EQ(clause1.type_, DataStructures::ClauseType::Merge);
//  EXPECT_EQ(clause1.elements_.size(), 2);
//  EXPECT_EQ(ds.Clauses().size(), 1);
//}
