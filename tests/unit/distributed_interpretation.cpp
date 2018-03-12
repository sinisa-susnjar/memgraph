#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "distributed_common.hpp"
#include "query/interpreter.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"
#include "utils/timer.hpp"

using namespace distributed;
using namespace database;

class DistributedInterpretationTest : public DistributedGraphDbTest {
 protected:
  auto Run(const std::string &query) {
    std::map<std::string, query::TypedValue> params = {};
    GraphDbAccessor dba(master());
    ResultStreamFaker result;
    query::Interpreter interpreter_;
    interpreter_(query, dba, params, false).PullAll(result);
    dba.Commit();
    return result.GetResults();
  }
};

TEST_F(DistributedInterpretationTest, RemotePullTest) {
  auto results = Run("OPTIONAL MATCH(n) UNWIND(RANGE(0, 20)) AS X RETURN 1");
  ASSERT_EQ(results.size(), 3 * 21);

  for (auto result : results) {
    ASSERT_EQ(result.size(), 1U);
    ASSERT_EQ(result[0].ValueInt(), 1);
  }
}

TEST_F(DistributedInterpretationTest, RemotePullNoResultsTest) {
  auto results = Run("MATCH (n) RETURN n");
  ASSERT_EQ(results.size(), 0U);
}

TEST_F(DistributedInterpretationTest, CreateExpand) {
  InsertVertex(master());
  InsertVertex(worker(1));
  InsertVertex(worker(1));
  InsertVertex(worker(2));
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  Run("MATCH (n) CREATE (n)-[:T]->(m) RETURN n");

  EXPECT_EQ(VertexCount(master()), 2);
  EXPECT_EQ(VertexCount(worker(1)), 4);
  EXPECT_EQ(VertexCount(worker(2)), 6);
}

TEST_F(DistributedInterpretationTest, RemoteExpandTest2) {
  // Make a fully connected graph with vertices scattered across master and
  // worker storage.
  // Vertex count is low, because test gets exponentially slower. The expected
  // result size is ~ vertices^3, and then that is compared at the end in no
  // particular order which causes O(result_size^2) comparisons.
  int verts_per_storage = 3;
  std::vector<storage::VertexAddress> vertices;
  vertices.reserve(verts_per_storage * 3);
  auto add_vertices = [this, &vertices, &verts_per_storage](auto &db) {
    for (int i = 0; i < verts_per_storage; ++i)
      vertices.push_back(InsertVertex(db));
  };
  add_vertices(master());
  add_vertices(worker(1));
  add_vertices(worker(2));
  auto get_edge_type = [](int v1, int v2) {
    return std::to_string(v1) + "-" + std::to_string(v2);
  };
  std::vector<std::string> edge_types;
  edge_types.reserve(vertices.size() * vertices.size());
  for (size_t i = 0; i < vertices.size(); ++i) {
    for (size_t j = 0; j < vertices.size(); ++j) {
      auto edge_type = get_edge_type(i, j);
      edge_types.push_back(edge_type);
      InsertEdge(vertices[i], vertices[j], edge_type);
    }
  }

  auto results = Run("MATCH (n)-[r1]-(m)-[r2]-(l) RETURN type(r1), type(r2)");
  // We expect the number of results to be:
  size_t expected_result_size =
      // pick (n)
      vertices.size() *
      // pick both directed edges to other (m) and a
      // single edge to (m) which equals (n), hence -1
      (2 * vertices.size() - 1) *
      // Pick as before, but exclude the previously taken edge, hence another -1
      (2 * vertices.size() - 1 - 1);
  std::vector<std::vector<std::string>> expected;
  expected.reserve(expected_result_size);
  for (size_t n = 0; n < vertices.size(); ++n) {
    for (size_t m = 0; m < vertices.size(); ++m) {
      std::vector<std::string> r1s{get_edge_type(n, m)};
      if (n != m) r1s.push_back(get_edge_type(m, n));
      for (size_t l = 0; l < vertices.size(); ++l) {
        std::vector<std::string> r2s{get_edge_type(m, l)};
        if (m != l) r2s.push_back(get_edge_type(l, m));
        for (const auto &r1 : r1s) {
          for (const auto &r2 : r2s) {
            if (r1 == r2) continue;
            expected.push_back({r1, r2});
          }
        }
      }
    }
  }
  ASSERT_EQ(expected.size(), expected_result_size);
  ASSERT_EQ(results.size(), expected_result_size);
  std::vector<std::vector<std::string>> got;
  got.reserve(results.size());
  for (const auto &res : results) {
    std::vector<std::string> row;
    row.reserve(res.size());
    for (const auto &col : res) {
      row.push_back(col.Value<std::string>());
    }
    got.push_back(row);
  }
  ASSERT_THAT(got, testing::UnorderedElementsAreArray(expected));
}

TEST_F(DistributedInterpretationTest, Cartesian) {
  // Create some data on the master and both workers.
  storage::Property prop;
  {
    GraphDbAccessor dba{master()};
    auto tx_id = dba.transaction_id();
    GraphDbAccessor dba1{worker(1), tx_id};
    GraphDbAccessor dba2{worker(2), tx_id};
    prop = dba.Property("prop");
    auto add_data = [prop](GraphDbAccessor &dba, int value) {
      dba.InsertVertex().PropsSet(prop, value);
    };

    for (int i = 0; i < 10; ++i) add_data(dba, i);
    for (int i = 10; i < 20; ++i) add_data(dba1, i);
    for (int i = 20; i < 30; ++i) add_data(dba2, i);

    dba.Commit();
  }

  std::vector<std::vector<int64_t>> expected;
  for (int64_t i = 0; i < 30; ++i)
    for (int64_t j = 0; j < 30; ++j) expected.push_back({i, j});

  auto results = Run("MATCH (n), (m) RETURN n.prop, m.prop;");

  size_t expected_result_size = 30 * 30;
  ASSERT_EQ(expected.size(), expected_result_size);
  ASSERT_EQ(results.size(), expected_result_size);

  std::vector<std::vector<int64_t>> got;
  got.reserve(results.size());
  for (const auto &res : results) {
    std::vector<int64_t> row;
    row.reserve(res.size());
    for (const auto &col : res) {
      row.push_back(col.Value<int64_t>());
    }
    got.push_back(row);
  }

  ASSERT_THAT(got, testing::UnorderedElementsAreArray(expected));
}

class TestQueryWaitsOnFutures : public DistributedInterpretationTest {
 protected:
  int QueryExecutionTimeSec(int worker_id) override {
    return worker_id == 2 ? 3 : 1;
  }
};

TEST_F(TestQueryWaitsOnFutures, Test) {
  const int kVertexCount = 10;
  auto make_fully_connected = [](database::GraphDb &db) {
    database::GraphDbAccessor dba(db);
    std::vector<VertexAccessor> vertices;
    for (int i = 0; i < kVertexCount; ++i)
      vertices.emplace_back(dba.InsertVertex());
    auto et = dba.EdgeType("et");
    for (auto &from : vertices)
      for (auto &to : vertices) dba.InsertEdge(from, to, et);
    dba.Commit();
  };

  make_fully_connected(worker(1));
  ASSERT_EQ(VertexCount(worker(1)), kVertexCount);
  ASSERT_EQ(EdgeCount(worker(1)), kVertexCount * kVertexCount);

  {
    utils::Timer timer;
    try {
      Run("MATCH ()--()--()--()--()--()--() RETURN count(1)");
    } catch (...) {
    }
    double seconds = timer.Elapsed().count();
    EXPECT_GT(seconds, 1);
    EXPECT_LT(seconds, 2);
  }

  make_fully_connected(worker(2));
  ASSERT_EQ(VertexCount(worker(2)), kVertexCount);
  ASSERT_EQ(EdgeCount(worker(2)), kVertexCount * kVertexCount);

  {
    utils::Timer timer;
    try {
      Run("MATCH ()--()--()--()--()--()--() RETURN count(1)");
    } catch (...) {
    }
    double seconds = timer.Elapsed().count();
    EXPECT_GT(seconds, 3);
  }
}
