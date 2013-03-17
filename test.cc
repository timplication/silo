#include <iostream>
#include <functional>
#include <unordered_map>
#include <tuple>

#include "thread.h"
#include "txn.h"
#include "btree.h"
#include "txn_btree.h"
#include "varint.h"
#include "xbuf.h"
#include "small_vector.h"
#include "small_unordered_map.h"
#include "counter.h"

#include "record/encoder.h"

#define MYREC_KEY_FIELDS(x, y) \
  x(int32_t,k0) \
  y(int32_t,k1)
#define MYREC_VALUE_FIELDS(x, y) \
  x(int32_t,v0) \
  y(int16_t,v1)
DO_STRUCT(myrec, MYREC_KEY_FIELDS, MYREC_VALUE_FIELDS)

using namespace std;
using namespace util;

static event_counter evt_test("test");
static event_counter evt_test1("test1");
static event_avg_counter evt_test_avg("test_avg");

namespace varkeytest {
  void
  Test()
  {
    const string s0 = u64_varkey(1).str();
    ALWAYS_ASSERT(s0.size() == 8);
    const char e0[] = { 0, 0, 0, 0, 0, 0, 0, 1 };
    ALWAYS_ASSERT(memcmp(s0.data(), &e0[0], 8) == 0);
    cout << "varkey test passed" << endl;
  }
}

namespace pxqueuetest {

  static inline void *
  N2P(uint64_t n)
  {
    return (void *) n;
  }

  static inline rcu::deleter_t
  N2D(uint64_t n)
  {
    return (rcu::deleter_t) n;
  }

  struct sorter {
    bool
    operator()(const rcu::delete_entry &a,
               const rcu::delete_entry &b) const
    {
      return (a.first < b.first) || (a.first == b.first && a.second < b.second);
    }
  };

  void
  Test()
  {
    typedef rcu::basic_px_queue<8> px_queue;

    {
      px_queue q0;
      for (size_t i = 0; i < 14; i++)
        q0.enqueue(0, N2P(i), N2D(i));
      ALWAYS_ASSERT(!q0.empty());
      q0.sanity_check();
      ALWAYS_ASSERT(!q0.all_epochs_past(0));
      ALWAYS_ASSERT(!q0.all_epochs_before(0));
      ALWAYS_ASSERT(q0.all_epochs_before(1));

      uint64_t i = 0;
      for (auto &p : q0) {
        ALWAYS_ASSERT(p.first == N2P(i));
        ALWAYS_ASSERT(p.second == N2D(i));
        i++;
      }
      ALWAYS_ASSERT(i == 14);

      px_queue q1;
      q1.enqueue(0, N2P(123), N2D(543));
      q1.enqueue(1, N2P(12345), N2D(54321));
      q1.sanity_check();
      auto q1_it = q1.begin();
      ALWAYS_ASSERT(q1_it != q1.end());
      ALWAYS_ASSERT(q1_it->first == N2P(123));
      ALWAYS_ASSERT(q1_it->second == N2D(543));
      ++q1_it;
      ALWAYS_ASSERT(q1_it != q1.end());
      ALWAYS_ASSERT(q1_it->first == N2P(12345));
      ALWAYS_ASSERT(q1_it->second == N2D(54321));
      ++q1_it;
      ALWAYS_ASSERT(q1_it == q1.end());

      q1.accept_from(q0, 0);
      INVARIANT(q0.empty());

      vector<rcu::delete_entry> v0;
      for (size_t i = 0; i < 14; i++)
        v0.emplace_back(N2P(i), N2D(i));
      v0.emplace_back(N2P(123), N2D(543));
      v0.emplace_back(N2P(12345), N2D(54321));

      vector<rcu::delete_entry> v1(q1.begin(), q1.end());

      sort(v0.begin(), v0.end(), sorter());
      sort(v1.begin(), v1.end(), sorter());

      if (v0 != v1) {
        cerr << "v0:" << endl;
        for (auto &p : v0)
          cerr << "  " << uint64_t(p.first) << " : " << uint64_t(p.second) << endl;
        cerr << "v1:" << endl;
        for (auto &p : v1)
          cerr << "  " << uint64_t(p.first) << " : " << uint64_t(p.second) << endl;
      }
      ALWAYS_ASSERT(v0 == v1);
    }

    {
      px_queue q0, q1;
      q0.enqueue(1, N2P(1), N2D(1));
      q1.enqueue(0, N2P(0), N2D(0));
      q0.sanity_check();
      q1.sanity_check();

      q0.accept_from(q1, 1);
      ALWAYS_ASSERT(q1.empty());

      vector<rcu::delete_entry> v0(q0.begin(), q0.end());
      vector<rcu::delete_entry> vtest = {{N2P(0), N2D(0)}, {N2P(1), N2D(1)}};
      ALWAYS_ASSERT(v0 == vtest);

      q0.dequeue_all(0);
      ALWAYS_ASSERT(!q0.empty());
      q0.sanity_check();

      q0.clear();
      ALWAYS_ASSERT(q0.empty());
      q0.sanity_check();
    }

    {
      px_queue q0, q1;
      q0.enqueue(1, N2P(1), N2D(1));
      q0.enqueue(3, N2P(3), N2D(3));

      q1.enqueue(2, N2P(2), N2D(2));
      q1.enqueue(4, N2P(4), N2D(4));
      q1.enqueue(6, N2P(6), N2D(6));

      q0.sanity_check();
      q1.sanity_check();

      q0.accept_from(q1, 5);
      ALWAYS_ASSERT(!q1.empty());

      vector<rcu::delete_entry> v0(q0.begin(), q0.end());
      vector<rcu::delete_entry> v0test =
        {{N2P(1), N2D(1)},
         {N2P(2), N2D(2)},
         {N2P(3), N2D(3)},
         {N2P(4), N2D(4)}};
      ALWAYS_ASSERT(v0 == v0test);

      vector<rcu::delete_entry> v1(q1.begin(), q1.end());
      vector<rcu::delete_entry> v1test = {{N2P(6), N2D(6)}};
      ALWAYS_ASSERT(v1 == v1test);

      q0.transfer_freelist(q1);
      q0.sanity_check();
      q1.sanity_check();

      q1.transfer_freelist(q0);
      q0.sanity_check();
      q1.sanity_check();

      q0.dequeue_all(10);
      ALWAYS_ASSERT(q0.empty());
      q1.dequeue_all(10);
      ALWAYS_ASSERT(q1.empty());
      q0.sanity_check();
      q1.sanity_check();
    }

    cout << "pxqueue test passed" << endl;
  }
}

void
CounterTest()
{
#ifdef ENABLE_EVENT_COUNTERS
  ++evt_test;
  ++evt_test;
  evt_test_avg.offer(1);
  evt_test_avg.offer(2);
  evt_test_avg.offer(3);
  map<string, event_counter::counter_data> m = event_counter::get_all_counters();
  ALWAYS_ASSERT(m.find("test") != m.end());
  ALWAYS_ASSERT(m.find("test1") != m.end());
  ALWAYS_ASSERT(m.find("test_avg") != m.end());

  ALWAYS_ASSERT(m["test"].count == 2);
  ALWAYS_ASSERT(m["test1"].count == 0);
  ALWAYS_ASSERT(m["test_avg"].count == 3);
  ALWAYS_ASSERT(m["test_avg"].sum == 6);
  ALWAYS_ASSERT(m["test_avg"].max == 3);

  cout << "event counters test passed" << endl;
#endif
}

void
UtilTest()
{
  _static_assert(CACHELINE_SIZE == 64);
  _static_assert(LG_CACHELINE_SIZE == 6);

  const bool e0 = round_up<size_t, 1>(3) == 4;
  ALWAYS_ASSERT(e0);
  const bool e1 = round_down<size_t, 1>(3) == 2;
  ALWAYS_ASSERT(e1);
  const bool e2 = round_up<size_t, 1>(2) == 2;
  ALWAYS_ASSERT(e2);
  const bool e3 = round_down<size_t, 1>(2) == 2;
  ALWAYS_ASSERT(e3);
  const bool e4 = round_down<size_t, LG_CACHELINE_SIZE>(2) == 0;
  ALWAYS_ASSERT(e4);
  const bool e5 = round_down<size_t, LG_CACHELINE_SIZE>(CACHELINE_SIZE) == CACHELINE_SIZE;
  ALWAYS_ASSERT(e5);
  const bool e6 = round_down<size_t, LG_CACHELINE_SIZE>(CACHELINE_SIZE + 10) == CACHELINE_SIZE;
  ALWAYS_ASSERT(e6);

  cout << "util test passed" << endl;
}

void
XbufTest()
{
  xbuf s = "hello";
  ALWAYS_ASSERT(s.size() == 5);
  ALWAYS_ASSERT(strncmp(s.data(), "hello", 5) == 0);
  xbuf y = s;
  ALWAYS_ASSERT(s == y);
  vector<xbuf> v;
  for (size_t i = 0; i < 5; i++)
    v.push_back("hi");
  unordered_map<xbuf, int> m;
  m["foo"] = 10;
  m["bar"] = 20;
  ostringstream buf;
  buf << s;
  ALWAYS_ASSERT(buf.str().size() == s.size());

  xbuf a = "hello";
  xbuf b = "world";
  swap(a, b);
  ALWAYS_ASSERT(a == "world");
  ALWAYS_ASSERT(b == "hello");

  xbuf t0;
  t0.resize(10, 'a');
  ALWAYS_ASSERT(t0.size() == 10);
  ALWAYS_ASSERT(t0 == xbuf("aaaaaaaaaa"));
  t0.append("foobar", 6);
  ALWAYS_ASSERT(t0 == xbuf("aaaaaaaaaafoobar"));
  t0.resize(5);
  ALWAYS_ASSERT(t0 == xbuf("aaaaa"));

  cout << xbuf("xbuf test passed") << endl;
}

namespace small_vector_ns {

typedef small_vector<string, 4> vec_type;
typedef vector<string> stl_vec_type;

template <typename VecType>
static void
init_vec0(VecType &v)
{
  ALWAYS_ASSERT(v.empty());
  ALWAYS_ASSERT(v.size() == 0);

  v.push_back("a");
  ALWAYS_ASSERT(!v.empty());
  ALWAYS_ASSERT(v.size() == 1);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "a");

  v.push_back("b");
  ALWAYS_ASSERT(v.size() == 2);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "b");

  v.push_back("c");
  ALWAYS_ASSERT(v.size() == 3);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "c");

  v.push_back("d");
  ALWAYS_ASSERT(v.size() == 4);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "d");
}

template <typename VecType>
static void
init_vec1(VecType &v)
{
  ALWAYS_ASSERT(v.empty());
  ALWAYS_ASSERT(v.size() == 0);

  v.push_back("a");
  ALWAYS_ASSERT(!v.empty());
  ALWAYS_ASSERT(v.size() == 1);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "a");

  v.push_back("b");
  ALWAYS_ASSERT(v.size() == 2);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "b");

  v.push_back("c");
  ALWAYS_ASSERT(v.size() == 3);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "c");

  v.push_back("d");
  ALWAYS_ASSERT(v.size() == 4);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "d");

  v.push_back("e");
  ALWAYS_ASSERT(v.size() == 5);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "e");

  v.push_back("f");
  ALWAYS_ASSERT(v.size() == 6);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "f");
}

template <typename VecA, typename VecB>
static void
assert_vecs_equal(VecA &v, const VecB &stl_v)
{
  ALWAYS_ASSERT(v.size() == stl_v.size());
  VecB tmp(v.begin(), v.end());
  ALWAYS_ASSERT(tmp == stl_v);
  const VecA &cv = v;
  VecB tmp1(cv.begin(), cv.end());
  ALWAYS_ASSERT(tmp1 == stl_v);
}

struct foo {
  int a;
  int b;
  int c;
  int d;

  foo()
    : a(), b(), c(), d()
  {}

  foo(int a, int b, int c, int d)
    : a(a), b(b), c(c), d(d)
  {}
};

struct PComp {
  inline bool
  operator()(const pair<uint32_t, uint32_t> &a,
             const pair<uint32_t, uint32_t> &b) const
  {
    return a.first < b.first;
  }
};

void
Test()
{
  {
    vec_type v;
    stl_vec_type stl_v;
    init_vec0(v);
    init_vec0(stl_v);
    vec_type v_copy(v);
    vec_type v_assign;
    ALWAYS_ASSERT(v_assign.empty());
    v_assign = v;
    assert_vecs_equal(v, stl_v);
    assert_vecs_equal(v_copy, stl_v);
    assert_vecs_equal(v_assign, stl_v);
    v.clear();
    assert_vecs_equal(v, stl_vec_type());
  }

  {
    vec_type v;
    stl_vec_type stl_v;
    init_vec1(v);
    init_vec1(stl_v);
    vec_type v_copy(v);
    vec_type v_assign;
    ALWAYS_ASSERT(v_assign.empty());
    v_assign = v;
    assert_vecs_equal(v, stl_v);
    assert_vecs_equal(v_copy, stl_v);
    assert_vecs_equal(v_assign, stl_v);
    v.clear();
    assert_vecs_equal(v, stl_vec_type());
  }

  {
    for (int iter = 0; iter < 10; iter++) {
      small_vector<foo> v;
      for (int i = 0; i < 20; i++) {
        v.push_back(foo(i, i + 1, i + 2, i + 3));
        ALWAYS_ASSERT(v.back().a == i);
        ALWAYS_ASSERT(v.back().b == (i + 1));
        ALWAYS_ASSERT(v.back().c == (i + 2));
        ALWAYS_ASSERT(v.back().d == (i + 3));
        ALWAYS_ASSERT(v[i].a == i);
        ALWAYS_ASSERT(v[i].b == (i + 1));
        ALWAYS_ASSERT(v[i].c == (i + 2));
        ALWAYS_ASSERT(v[i].d == (i + 3));
      }
      for (int i = 0; i < 20; i++) {
        ALWAYS_ASSERT(v[i].a == i);
        ALWAYS_ASSERT(v[i].b == (i + 1));
        ALWAYS_ASSERT(v[i].c == (i + 2));
        ALWAYS_ASSERT(v[i].d == (i + 3));
      }
    }
  }

  {
    small_vector<int> v;
    v.push_back(10);
    v.push_back(2);
    v.push_back(5);
    v.push_back(7);
    v.push_back(3);
    v.push_back(100);
    sort(v.begin(), v.end());

    vector<int> stl_v;
    stl_v.push_back(10);
    stl_v.push_back(2);
    stl_v.push_back(5);
    stl_v.push_back(7);
    stl_v.push_back(3);
    stl_v.push_back(100);
    sort(stl_v.begin(), stl_v.end());

    assert_vecs_equal(v, stl_v);
  }

  {
    small_vector<int, 3> v;
    v.push_back(10);
    v.push_back(2);
    v.push_back(5);
    v.push_back(7);
    v.push_back(3);
    v.push_back(100);
    sort(v.begin(), v.end());

    vector<int> stl_v;
    stl_v.push_back(10);
    stl_v.push_back(2);
    stl_v.push_back(5);
    stl_v.push_back(7);
    stl_v.push_back(3);
    stl_v.push_back(100);
    sort(stl_v.begin(), stl_v.end());

    assert_vecs_equal(v, stl_v);
  }

  {
    fast_random r(29395);
    small_vector< pair<uint32_t, uint32_t> > v;
    vector< pair<uint32_t, uint32_t> > stl_v;
    for (size_t i = 0; i < 48; i++) {
      uint32_t x = r.next();
      v.push_back(make_pair(x, x + 1));
      stl_v.push_back(make_pair(x, x + 1));
    }
    sort(v.begin(), v.end(), PComp());
    sort(stl_v.begin(), stl_v.end(), PComp());
    assert_vecs_equal(v, stl_v);
    for (size_t i = 0; i < 48; i++)
      ALWAYS_ASSERT(v[i].first + 1 == v[i].second);
  }

  {
    // test C++11 features
    small_vector<string, 4> v;
    v.emplace_back("hello");
    string world = "world";
    v.push_back(move(world));
    //ALWAYS_ASSERT(world.empty());
    ALWAYS_ASSERT(v.size() == 2);
    ALWAYS_ASSERT(v[0] == "hello");
    ALWAYS_ASSERT(v[1] == "world");
  }

  cout << "vec test passed" << endl;
}

}

namespace small_map_ns {

typedef small_unordered_map<string, string, 4> map_type;
typedef map<string, string> stl_map_type;

static void
assert_map_contains(map_type &m, const string &k, const string &v)
{
  ALWAYS_ASSERT(!m.empty());
  ALWAYS_ASSERT(m[k] == v);
  {
    map_type::iterator it = m.find(k);
    ALWAYS_ASSERT(it != m.end());
    ALWAYS_ASSERT(it->first == k);
    ALWAYS_ASSERT(it->second == v);
  }
  const map_type &const_m = m;
  {
    map_type::const_iterator it = const_m.find(k);
    ALWAYS_ASSERT(it != const_m.end());
    ALWAYS_ASSERT(it->first == k);
    ALWAYS_ASSERT(it->second == v);
  }
}

static void
assert_map_equals(map_type &m, const stl_map_type &stl_m)
{
  ALWAYS_ASSERT(m.size() == stl_m.size());

  // reg version prefix
  {
    stl_map_type test;
    for (map_type::iterator it = m.begin();
         it != m.end(); ++it) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // reg version postfix
  {
    stl_map_type test;
    for (map_type::iterator it = m.begin();
         it != m.end(); it++) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // const version prefix
  {
    const map_type &const_m = m;
    stl_map_type test;
    for (map_type::const_iterator it = const_m.begin();
         it != const_m.end(); ++it) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // const version postfix
  {
    const map_type &const_m = m;
    stl_map_type test;
    for (map_type::const_iterator it = const_m.begin();
         it != const_m.end(); it++) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }
}

template <typename T>
static void
init_map(T& m)
{
  m["a"] = "1";
  m["b"] = "2";
  m["c"] = "3";
  m["d"] = "4";
}

template <typename T>
static void
init_map1(T& m)
{
  m["a"] = "1";
  m["b"] = "2";
  m["c"] = "3";
  m["d"] = "4";
  m["e"] = "5";
  m["f"] = "6";
}

void
Test()
{
  {
    map_type m, m_copy;
    stl_map_type stl_m;
    init_map(m);
    ALWAYS_ASSERT(m.is_small_type());
    INVARIANT(m.size() == 4);
    init_map(stl_m);
    ALWAYS_ASSERT(m_copy.is_small_type());
    m_copy = m;
    ALWAYS_ASSERT(m_copy.is_small_type());
    INVARIANT(m_copy.size() == 4);
    map_type m_construct(m);
    INVARIANT(m_construct.size() == 4);
    for (stl_map_type::iterator it = stl_m.begin();
         it != stl_m.end(); ++it) {
      assert_map_contains(m, it->first, it->second);
      assert_map_contains(m_copy, it->first, it->second);
      assert_map_contains(m_construct, it->first, it->second);
    }
    assert_map_equals(m, stl_m);
    assert_map_equals(m_copy, stl_m);
    assert_map_equals(m_construct, stl_m);
    ALWAYS_ASSERT(m.is_small_type());
    ALWAYS_ASSERT(m_copy.is_small_type());
    ALWAYS_ASSERT(m_construct.is_small_type());
  }

  {
    map_type m, m_copy;
    stl_map_type stl_m;
    init_map1(m);
    init_map1(stl_m);
    m_copy = m;
    map_type m_construct(m);
    for (stl_map_type::iterator it = stl_m.begin();
         it != stl_m.end(); ++it) {
      assert_map_contains(m, it->first, it->second);
      assert_map_contains(m_copy, it->first, it->second);
    }
    assert_map_equals(m, stl_m);
    assert_map_equals(m_copy, stl_m);
    assert_map_equals(m_construct, stl_m);
  }

  {
    map_type m;
    ALWAYS_ASSERT(m.empty());
    ALWAYS_ASSERT(m.size() == 0);
    m["a"] = "1";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 1);
    m["b"] = "2";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 2);
    m["b"] = "2";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 2);
    m["c"] = "3";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 3);
    m["d"] = "4";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 4);

    m.clear();
    ALWAYS_ASSERT(m.empty());
    ALWAYS_ASSERT(m.size() == 0);
    m["a"] = "1";
    m["b"] = "2";
    m["c"] = "3";
    m["d"] = "4";
    m["d"] = "4";
    m["d"] = "4";
    m["e"] = "5";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 5);
  }

  { // check primitive key type maps
    small_unordered_map<int, int> m;
    m[0] = 1;
    m[1] = 2;
    m[2] = 3;
    ALWAYS_ASSERT(m.find(0) != m.end());
    ALWAYS_ASSERT(m.find(1) != m.end());
    ALWAYS_ASSERT(m.find(2) != m.end());
    ALWAYS_ASSERT(m.find(0)->first == 0 && m.find(0)->second == 1);
    ALWAYS_ASSERT(m.find(1)->first == 1 && m.find(1)->second == 2);
    ALWAYS_ASSERT(m.find(2)->first == 2 && m.find(2)->second == 3);
  }

  cout << "map test passed" << endl;
}
}

namespace recordtest {

ostream &
operator<<(ostream &o, const myrec::key &k)
{
  o << "[k0=" << k.k0 << ", k1=" << k.k1 << "]" << endl;
  return o;
}

ostream &
operator<<(ostream &o, const myrec::value &v)
{
  o << "[v0=" << v.v0 << ", v1=" << v.v1 << "]" << endl;
  return o;
}

void
Test()
{
  const myrec::key k0(123, 456);
  const myrec::key k1(999, 123);
  const myrec::key k2(123, 457);
  myrec::key k0_temp, k1_temp, k2_temp;

  ALWAYS_ASSERT(Size(k0) == sizeof(k0));
  ALWAYS_ASSERT(Size(k1) == sizeof(k1));
  ALWAYS_ASSERT(Size(k2) == sizeof(k2));

  {
    const string s0 = Encode(k0);
    const string s1 = Encode(k1);
    const string s2 = Encode(k2);
    ALWAYS_ASSERT(s0 < s1);
    ALWAYS_ASSERT(s0 < s2);
    Decode(s0, k0_temp);
    Decode(s1, k1_temp);
    Decode(s2, k2_temp);
    ALWAYS_ASSERT(k0 == k0_temp);
    ALWAYS_ASSERT(k1 == k1_temp);
    ALWAYS_ASSERT(k2 == k2_temp);

    string t0, t1, t2;
    const myrec::key *p0 = Decode(Encode(k0), k0_temp);
    const myrec::key *p1 = Decode(Encode(k1), k1_temp);
    const myrec::key *p2 = Decode(Encode(k2), k2_temp);
    ALWAYS_ASSERT(*p0 == k0);
    ALWAYS_ASSERT(*p1 == k1);
    ALWAYS_ASSERT(*p2 == k2);
  }

  const myrec::value v0(859435, 2834);
  const myrec::value v1(0, 73);
  const myrec::value v2(654, 8);
  myrec::value v0_temp, v1_temp, v2_temp;

  {
    const size_t sz0 = Size(v0);
    const size_t sz1 = Size(v1);
    const size_t sz2 = Size(v2);
    uint8_t buf0[sz0], buf1[sz1], buf2[sz2];
    Encode(buf0, v0);
    Encode(buf1, v1);
    Encode(buf2, v2);

    const string s0((const char *) buf0, sz0);
    const string s1((const char *) buf1, sz1);
    const string s2((const char *) buf2, sz2);

    Decode(s0, v0_temp);
    Decode(s1, v1_temp);
    Decode(s2, v2_temp);
    ALWAYS_ASSERT(v0 == v0_temp);
    ALWAYS_ASSERT(v1 == v1_temp);
    ALWAYS_ASSERT(v2 == v2_temp);
  }

  cout << "encoder test passed" << endl;
}

}

class main_thread : public ndb_thread {
public:
  main_thread(int argc, char **argv)
    : ndb_thread(false, string("main")),
      argc(argc), argv(argv), ret(0)
  {}

  virtual void
  run()
  {
#ifndef CHECK_INVARIANTS
    cerr << "WARNING: tests are running without invariant checking" << endl;
#endif
    varkeytest::Test();
    pxqueuetest::Test();
    CounterTest();
    UtilTest();
    XbufTest();
    varint::Test();
    small_vector_ns::Test();
    small_map_ns::Test();
    recordtest::Test();
    btree::TestFast();
    //btree::TestSlow();
    txn_btree_test();
    ret = 0;
  }

  inline int
  retval() const
  {
    return ret;
  }
private:
  const int argc;
  char **const argv;
  volatile int ret;
};

int
main(int argc, char **argv)
{
  main_thread t(argc, argv);
  t.start();
  t.join();
  return t.retval();
}
