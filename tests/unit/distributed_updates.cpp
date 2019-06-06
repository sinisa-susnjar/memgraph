#include <functional>
#include <unordered_map>

#include <gtest/gtest.h>

#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "query/typed_value.hpp"
#include "storage/common/types/property_value.hpp"

#include "distributed_common.hpp"

class DistributedUpdateTest : public DistributedGraphDbTest {
 protected:
  DistributedUpdateTest() : DistributedGraphDbTest("update") {}

  std::unique_ptr<database::GraphDbAccessor> dba1;
  std::unique_ptr<database::GraphDbAccessor> dba2;
  storage::Label label;
  std::unique_ptr<VertexAccessor> v1_dba1;
  std::unique_ptr<VertexAccessor> v1_dba2;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();

    auto dba_tx1 = worker(1).Access();
    auto v = dba_tx1->InsertVertex();
    auto v_ga = v.GlobalAddress();
    dba_tx1->Commit();

    dba1 = worker(1).Access();
    dba2 = worker(2).Access(dba1->transaction_id());

    v1_dba1 = std::make_unique<VertexAccessor>(v_ga, *dba1);
    v1_dba2 = std::make_unique<VertexAccessor>(v_ga, *dba2);
    ASSERT_FALSE(v1_dba2->address().is_local());
    label = dba1->Label("l");
    v1_dba2->add_label(label);
  }

  void TearDown() override {
    dba2 = nullptr;
    dba1 = nullptr;
    DistributedGraphDbTest::TearDown();
  }
};

#define EXPECT_LABEL(var, old_result, new_result) \
  {                                               \
    var->SwitchOld();                             \
    EXPECT_EQ(var->has_label(label), old_result); \
    var->SwitchNew();                             \
    EXPECT_EQ(var->has_label(label), new_result); \
  }

TEST_F(DistributedUpdateTest, UpdateLocalOnly) {
  EXPECT_LABEL(v1_dba2, false, true);
  EXPECT_LABEL(v1_dba1, false, false);
}

TEST_F(DistributedUpdateTest, UpdateApply) {
  EXPECT_LABEL(v1_dba1, false, false);
  worker(1).updates_server().Apply(dba1->transaction_id());
  EXPECT_LABEL(v1_dba1, false, true);
}

#undef EXPECT_LABEL

class DistributedGraphDbSimpleUpdatesTest : public DistributedGraphDbTest {
 public:
  DistributedGraphDbSimpleUpdatesTest()
      : DistributedGraphDbTest("simple_updates") {}
};

TEST_F(DistributedGraphDbSimpleUpdatesTest, CreateVertex) {
  gid::Gid gid;
  {
    auto dba = worker(1).Access();
    auto v =
        database::InsertVertexIntoRemote(dba.get(), 2, {}, {}, std::nullopt);
    gid = v.gid();
    dba->Commit();
  }
  {
    auto dba = worker(2).Access();
    auto v = dba->FindVertexOptional(gid, false);
    ASSERT_TRUE(v);
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, CreateVertexWithUpdate) {
  gid::Gid gid;
  storage::Property prop;
  {
    auto dba = worker(1).Access();
    auto v =
        database::InsertVertexIntoRemote(dba.get(), 2, {}, {}, std::nullopt);
    gid = v.gid();
    prop = dba->Property("prop");
    v.PropsSet(prop, 42);
    worker(2).updates_server().Apply(dba->transaction_id());
    dba->Commit();
  }
  {
    auto dba = worker(2).Access();
    auto v = dba->FindVertexOptional(gid, false);
    ASSERT_TRUE(v);
    EXPECT_EQ(v->PropsAt(prop).Value<int64_t>(), 42);
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, CreateVertexWithData) {
  gid::Gid gid;
  storage::Label l1;
  storage::Label l2;
  storage::Property prop;
  {
    auto dba = worker(1).Access();
    l1 = dba->Label("l1");
    l2 = dba->Label("l2");
    prop = dba->Property("prop");
    auto v = database::InsertVertexIntoRemote(dba.get(), 2, {l1, l2},
                                              {{prop, 42}}, std::nullopt);
    gid = v.gid();

    // Check local visibility before commit.
    EXPECT_TRUE(v.has_label(l1));
    EXPECT_TRUE(v.has_label(l2));
    EXPECT_EQ(v.PropsAt(prop).Value<int64_t>(), 42);

    worker(2).updates_server().Apply(dba->transaction_id());
    dba->Commit();
  }
  {
    auto dba = worker(2).Access();
    auto v = dba->FindVertexOptional(gid, false);
    ASSERT_TRUE(v);
    // Check remote data after commit.
    EXPECT_TRUE(v->has_label(l1));
    EXPECT_TRUE(v->has_label(l2));
    EXPECT_EQ(v->PropsAt(prop).Value<int64_t>(), 42);
  }
}

// Checks if expiring a local record for a local update before applying a remote
// update delta causes a problem
TEST_F(DistributedGraphDbSimpleUpdatesTest, UpdateVertexRemoteAndLocal) {
  gid::Gid gid;
  storage::Label l1;
  storage::Label l2;
  {
    auto dba = worker(1).Access();
    auto v = dba->InsertVertex();
    gid = v.gid();
    l1 = dba->Label("label1");
    l2 = dba->Label("label2");
    dba->Commit();
  }
  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto v_local = dba1->FindVertex(gid, false);
    auto v_remote = VertexAccessor(storage::VertexAddress(gid, 1), *dba0);

    v_remote.add_label(l2);
    v_local.add_label(l1);

    auto result = worker(1).updates_server().Apply(dba0->transaction_id());
    EXPECT_EQ(result, distributed::UpdateResult::DONE);
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, AddSameLabelRemoteAndLocal) {
  auto v_address = InsertVertex(worker(1));
  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto v_local = dba1->FindVertex(v_address.gid(), false);
    auto v_remote = VertexAccessor(v_address, *dba0);
    auto l1 = dba1->Label("label");
    v_remote.add_label(l1);
    v_local.add_label(l1);
    worker(1).updates_server().Apply(dba0->transaction_id());
    dba0->Commit();
  }
  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto v = dba1->FindVertex(v_address.gid(), false);
    EXPECT_EQ(v.labels().size(), 1);
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, IndexGetsUpdatedRemotely) {
  storage::VertexAddress v_remote = InsertVertex(worker(1));
  storage::Label label;
  {
    auto dba0 = master().Access();
    label = dba0->Label("label");
    VertexAccessor va(v_remote, *dba0);
    va.add_label(label);
    worker(1).updates_server().Apply(dba0->transaction_id());
    dba0->Commit();
  }
  {
    auto dba1 = worker(1).Access();
    auto vertices = dba1->Vertices(label, false);
    EXPECT_EQ(std::distance(vertices.begin(), vertices.end()), 1);
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, DeleteVertexRemoteCommit) {
  auto v_address = InsertVertex(worker(1));
  auto dba0 = master().Access();
  auto dba1 = worker(1).Access(dba0->transaction_id());
  auto v_remote = VertexAccessor(v_address, *dba0);
  dba0->RemoveVertex(v_remote);
  EXPECT_TRUE(dba1->FindVertexOptional(v_address.gid(), true));
  EXPECT_EQ(worker(1).updates_server().Apply(dba0->transaction_id()),
            distributed::UpdateResult::DONE);
  EXPECT_FALSE(dba1->FindVertexOptional(v_address.gid(), true));
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, DeleteVertexRemoteBothDelete) {
  auto v_address = InsertVertex(worker(1));
  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto v_local = dba1->FindVertex(v_address.gid(), false);
    auto v_remote = VertexAccessor(v_address, *dba0);
    EXPECT_TRUE(dba1->RemoveVertex(v_local));
    EXPECT_TRUE(dba0->RemoveVertex(v_remote));
    EXPECT_EQ(worker(1).updates_server().Apply(dba0->transaction_id()),
              distributed::UpdateResult::DONE);
    EXPECT_FALSE(dba1->FindVertexOptional(v_address.gid(), true));
  }
}

TEST_F(DistributedGraphDbSimpleUpdatesTest, DeleteVertexRemoteStillConnected) {
  auto v_address = InsertVertex(worker(1));
  auto e_address = InsertEdge(v_address, v_address, "edge");

  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto v_remote = VertexAccessor(v_address, *dba0);
    dba0->RemoveVertex(v_remote);
    EXPECT_EQ(worker(1).updates_server().Apply(dba0->transaction_id()),
              distributed::UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR);
    EXPECT_TRUE(dba1->FindVertexOptional(v_address.gid(), true));
  }
  {
    auto dba0 = master().Access();
    auto dba1 = worker(1).Access(dba0->transaction_id());
    auto e_local = dba1->FindEdge(e_address.gid(), false);
    EXPECT_TRUE(dba1->FindVertexOptional(v_address.gid(), false));
    auto v_remote = VertexAccessor(v_address, *dba0);

    dba1->RemoveEdge(e_local);
    dba0->RemoveVertex(v_remote);

    EXPECT_EQ(worker(1).updates_server().Apply(dba0->transaction_id()),
              distributed::UpdateResult::DONE);
    EXPECT_FALSE(dba1->FindVertexOptional(v_address.gid(), true));
  }
}

class DistributedDetachDeleteTest : public DistributedGraphDbTest {
 protected:
  DistributedDetachDeleteTest() : DistributedGraphDbTest("detach_delete") {}

  storage::VertexAddress w1_a;
  storage::VertexAddress w1_b;
  storage::VertexAddress w2_a;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();
    w1_a = InsertVertex(worker(1));
    w1_b = InsertVertex(worker(1));
    w2_a = InsertVertex(worker(2));
  }

  template <typename TF>
  void Run(storage::VertexAddress v_address, TF check_func) {
    for (int i : {0, 1, 2}) {
      auto dba0 = master().Access();
      auto dba1 = worker(1).Access(dba0->transaction_id());
      auto dba2 = worker(2).Access(dba0->transaction_id());

      std::vector<std::reference_wrapper<database::GraphDbAccessor>> dba{
          *dba0, *dba1, *dba2};
      std::vector<database::GraphDb *> dbs{&master(), &worker(1),
                                                      &worker(2)};

      auto &accessor = dba[i].get();
      auto v_accessor = VertexAccessor(v_address, accessor);
      accessor.DetachRemoveVertex(v_accessor);

      for (auto *db : dbs) {
        ASSERT_EQ(db->updates_server().Apply(dba[0].get().transaction_id()),
                  distributed::UpdateResult::DONE);
      }

      check_func(dba);
    }
  }
};

TEST_F(DistributedDetachDeleteTest, VertexCycle) {
  auto e_address = InsertEdge(w1_a, w1_a, "edge");
  Run(w1_a,
      [this, e_address](
          std::vector<std::reference_wrapper<database::GraphDbAccessor>> &dba) {
        EXPECT_FALSE(dba[1].get().FindVertexOptional(w1_a.gid(), true));
        EXPECT_FALSE(dba[1].get().FindEdgeOptional(e_address.gid(), true));
      });
}

TEST_F(DistributedDetachDeleteTest, TwoVerticesDifferentWorkers) {
  auto e_address = InsertEdge(w1_a, w2_a, "edge");

  // Delete from
  Run(w1_a,
      [this, e_address](
          std::vector<std::reference_wrapper<database::GraphDbAccessor>> &dba) {
        EXPECT_FALSE(dba[1].get().FindVertexOptional(w1_a.gid(), true));
        EXPECT_TRUE(dba[2].get().FindVertexOptional(w2_a.gid(), true));
        EXPECT_FALSE(dba[1].get().FindEdgeOptional(e_address.gid(), true));
      });

  // Delete to
  Run(w2_a,
      [this, e_address](
          std::vector<std::reference_wrapper<database::GraphDbAccessor>> &dba) {
        EXPECT_TRUE(dba[1].get().FindVertexOptional(w1_a.gid(), true));
        EXPECT_FALSE(dba[2].get().FindVertexOptional(w2_a.gid(), true));
        EXPECT_FALSE(dba[1].get().FindEdgeOptional(e_address.gid(), true));
      });
}

TEST_F(DistributedDetachDeleteTest, TwoVerticesSameWorkers) {
  auto e_address = InsertEdge(w1_a, w1_b, "edge");

  // Delete from
  Run(w1_a,
      [this, e_address](
          std::vector<std::reference_wrapper<database::GraphDbAccessor>> &dba) {
        EXPECT_FALSE(dba[1].get().FindVertexOptional(w1_a.gid(), true));
        EXPECT_TRUE(dba[1].get().FindVertexOptional(w1_b.gid(), true));
        EXPECT_FALSE(dba[1].get().FindEdgeOptional(e_address.gid(), true));
      });

  // Delete to
  Run(w1_b,
      [this, e_address](
          std::vector<std::reference_wrapper<database::GraphDbAccessor>> &dba) {
        EXPECT_TRUE(dba[1].get().FindVertexOptional(w1_a.gid(), true));
        EXPECT_FALSE(dba[1].get().FindVertexOptional(w1_b.gid(), true));
        EXPECT_FALSE(dba[1].get().FindEdgeOptional(e_address.gid(), true));
      });
}

class DistributedEdgeCreateTest : public DistributedGraphDbTest {
 protected:
  DistributedEdgeCreateTest() : DistributedGraphDbTest("edge_create") {}

  storage::VertexAddress w1_a;
  storage::VertexAddress w1_b;
  storage::VertexAddress w2_a;
  std::unordered_map<std::string, PropertyValue> props{{"p1", 42},
                                                       {"p2", true}};
  storage::EdgeAddress e_ga;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();
    w1_a = InsertVertex(worker(1));
    w1_b = InsertVertex(worker(1));
    w2_a = InsertVertex(worker(2));
  }

  void CreateEdge(database::GraphDb &creator, storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr) {
    CHECK(from_addr.is_remote() && to_addr.is_remote())
        << "Local address given to CreateEdge";
    auto dba = creator.Access();
    auto edge_type = dba->EdgeType("et");
    VertexAccessor v1{from_addr, *dba};
    VertexAccessor v2{to_addr, *dba};
    auto edge = dba->InsertEdge(v1, v2, edge_type);
    e_ga = edge.GlobalAddress();

    for (auto &kv : props) edge.PropsSet(dba->Property(kv.first), kv.second);

    master().updates_server().Apply(dba->transaction_id());
    worker(1).updates_server().Apply(dba->transaction_id());
    worker(2).updates_server().Apply(dba->transaction_id());
    dba->Commit();
  }

  void CheckState(database::GraphDb &db, bool edge_is_local,
                  storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr) {
    auto dba = db.Access();

    // Check edge data.
    {
      EdgeAccessor edge{e_ga, *dba};
      EXPECT_EQ(edge.address().is_local(), edge_is_local);
      EXPECT_EQ(edge.GlobalAddress(), e_ga);
      auto from = edge.from();
      EXPECT_EQ(from.GlobalAddress(), from_addr);
      EXPECT_EQ(edge.from_addr().is_local(), from.is_local());
      auto to = edge.to();
      EXPECT_EQ(to.GlobalAddress(), to_addr);
      EXPECT_EQ(edge.to_addr().is_local(), to.is_local());

      EXPECT_EQ(edge.Properties().size(), props.size());
      for (auto &kv : props) {
        auto equality =
            query::TypedValue(edge.PropsAt(dba->Property(kv.first))) ==
            query::TypedValue(kv.second);
        EXPECT_TRUE(equality.IsBool() && equality.ValueBool());
      }
    }

    auto edges = [](auto iterable) {
      std::vector<EdgeAccessor> res;
      for (auto edge : iterable) res.emplace_back(edge);
      return res;
    };

    // Check `from` data.
    {
      VertexAccessor from{from_addr, *dba};
      ASSERT_EQ(edges(from.out()).size(), 1);
      EXPECT_EQ(edges(from.out())[0].GlobalAddress(), e_ga);
      // In case of cycles we have 1 in the `in` edges.
      EXPECT_EQ(edges(from.in()).size(), from_addr == to_addr);
    }

    // Check `to` data.
    {
      VertexAccessor to{to_addr, *dba};
      // In case of cycles we have 1 in the `out` edges.
      EXPECT_EQ(edges(to.out()).size(), from_addr == to_addr);
      ASSERT_EQ(edges(to.in()).size(), 1);
      EXPECT_EQ(edges(to.in())[0].GlobalAddress(), e_ga);
    }
  }

  void CheckAll(storage::VertexAddress from_addr,
                storage::VertexAddress to_addr) {
    int edge_worker = from_addr.worker_id();
    EXPECT_EQ(EdgeCount(master()), edge_worker == 0);
    EXPECT_EQ(EdgeCount(worker(1)), edge_worker == 1);
    EXPECT_EQ(EdgeCount(worker(2)), edge_worker == 2);
    CheckState(master(), edge_worker == 0, from_addr, to_addr);
    CheckState(worker(1), edge_worker == 1, from_addr, to_addr);
    CheckState(worker(2), edge_worker == 2, from_addr, to_addr);
  }
};

TEST_F(DistributedEdgeCreateTest, LocalRemote) {
  CreateEdge(worker(1), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteLocal) {
  CreateEdge(worker(2), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteDifferentWorkers) {
  CreateEdge(master(), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteSameWorkers) {
  CreateEdge(master(), w1_a, w1_b);
  CheckAll(w1_a, w1_b);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteCycle) {
  CreateEdge(master(), w1_a, w1_a);
  CheckAll(w1_a, w1_a);
}

class DistributedEdgeRemoveTest : public DistributedGraphDbTest {
 protected:
  DistributedEdgeRemoveTest() : DistributedGraphDbTest("edge_remove") {}

  storage::VertexAddress from_addr;
  storage::VertexAddress to_addr;
  storage::EdgeAddress edge_addr;

  void Create(database::GraphDb &from_db, database::GraphDb &to_db) {
    from_addr = InsertVertex(from_db);
    to_addr = InsertVertex(to_db);
    edge_addr = InsertEdge(from_addr, to_addr, "edge_type");
  }

  void Delete(database::GraphDb &db) {
    auto dba = db.Access();
    EdgeAccessor edge{edge_addr, *dba};
    dba->RemoveEdge(edge);
    master().updates_server().Apply(dba->transaction_id());
    worker(1).updates_server().Apply(dba->transaction_id());
    worker(2).updates_server().Apply(dba->transaction_id());
    dba->Commit();
  }

  template <typename TIterable>
  auto Size(TIterable iterable) {
    size_t size = 0;
    for (auto i = iterable.begin(); i != iterable.end(); ++i) {
      ++size;
    }
    return size;
  };

  void CheckCreation() {
    auto wid = from_addr.worker_id();
    ASSERT_TRUE(wid >= 0 && wid < 3);
    ASSERT_EQ(EdgeCount(master()), wid == 0);
    ASSERT_EQ(EdgeCount(worker(1)), wid == 1);
    ASSERT_EQ(EdgeCount(worker(2)), wid == 2);

    auto dba = master().Access();
    VertexAccessor from{from_addr, *dba};
    EXPECT_EQ(Size(from.out()), 1);
    EXPECT_EQ(Size(from.in()), 0);

    VertexAccessor to{to_addr, *dba};
    EXPECT_EQ(Size(to.out()), 0);
    EXPECT_EQ(Size(to.in()), 1);
  }

  void CheckDeletion() {
    EXPECT_EQ(EdgeCount(master()), 0);
    EXPECT_EQ(EdgeCount(worker(1)), 0);
    EXPECT_EQ(EdgeCount(worker(2)), 0);

    auto dba = master().Access();

    VertexAccessor from{from_addr, *dba};
    EXPECT_EQ(Size(from.out()), 0);
    EXPECT_EQ(Size(from.in()), 0);

    VertexAccessor to{to_addr, *dba};
    EXPECT_EQ(Size(to.out()), 0);
    EXPECT_EQ(Size(to.in()), 0);
  }
};

TEST_F(DistributedEdgeRemoveTest, DifferentVertexOwnersRemoteDelete) {
  Create(worker(1), worker(2));
  CheckCreation();
  Delete(master());
  CheckDeletion();
}

TEST_F(DistributedEdgeRemoveTest, DifferentVertexOwnersFromDelete) {
  Create(worker(1), worker(2));
  CheckCreation();
  Delete(worker(1));
  CheckDeletion();
}

TEST_F(DistributedEdgeRemoveTest, DifferentVertexOwnersToDelete) {
  Create(worker(1), worker(2));
  CheckCreation();
  Delete(worker(2));
  CheckDeletion();
}

TEST_F(DistributedEdgeRemoveTest, SameVertexOwnersRemoteDelete) {
  Create(worker(1), worker(1));
  CheckCreation();
  Delete(worker(2));
  CheckDeletion();
}
