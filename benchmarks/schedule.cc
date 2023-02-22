#include <iostream>
#include <sstream>
#include <utility>
#include <vector>
#include <map>
#include <string>

#include "../core.h"

#include "bench.h"
#include "schedule.h"

using namespace std;
using namespace util;

class schedule_loader : public bench_loader {
    public:
        schedule_loader(unsigned long seed, abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
            : bench_loader(seed, db, open_tables) {}

    protected:
        virtual void load() {
            string obj_buf;
            void *txn = db->new_txn(txn_flags, arena, txn_buf());

            // Warehouse Tuples
            // Warehouse(1, 80000)
            const warehouse::key w_k1;
            w_k1.w_id = 1;
            const warehouse::value w_v1;
            w_v1.w_ytd = 80000;
            open_tables["warehouse"]->insert(txn, Encode(w_k1), Encode(obj_buf, w_v1));

            // Warehouse(2, 1600000)
            const warehouse::key w_k2;
            w_k2.w_id = 2;
            const warehouse::value w_v1;
            w_v2.w_ytd = 1600000;
            open_tables["warehouse"]->insert(txn, Encode(w_k2), Encode(obj_buf, w_v2));

            // District Tuples
            // District(1, 1, 80000)
            const district::key d_k1;
            d_k1.d_w_id = 1;
            d_k1.d_id = 1;
            const district::value d_v1;
            d_v1.d_ytd = 80000;
            open_tables["district"]->insert(txn, Encode(d_k1), Encode(obj_buf, d_v1));

            // District(2, 2, 1600000)
            const district::key d_k2;
            d_k2.d_w_id = 2;
            d_k2.d_id = 2;
            const district::value d_v2;
            d_v2.d_ytd = 1600000;
            open_tables["district"]->insert(txn, Encode(d_k2), Encode(obj_buf, d_v2));

            // Customer Tuples
            // Customer(1, 1, 1, 1000)
            const customer::key c_k1;
            c_k1.c_w_id = 1;
            c_k1.c_d_id = 1;
            c_k1.c_id = 1;
            const customer::value c_v1;
            c_v1.c_balance = 1000;
            open_tables["customer"]->insert(txn, Encode(c_k1), Encode(obj_buf, c_v1));

            // Customer(2, 2, 2, 5000)
            const customer::key c_k2;
            c_k2.c_w_id = 2;
            c_k2.c_d_id = 2;
            c_k2.c_id = 2;
            const customer::value c_v2;
            c_v2.c_balance = 5000;
            open_tables["customer"]->insert(txn, Encode(c_k2), Encode(obj_buf, c_v2));

            // Order Tuple
            // Order(1, 1, 1, 1, 1)
            const order::key o_k1;
            o_k1.o_w_id = 1;
            o_k1.o_d_id = 1;
            o_k1.o_id = 1;
            const order::value o_v1;
            o_v1.o_c_id = 1;
            o_v1.o_carrier_id = 1;
            open_tables["order"]->insert(txn, Encode(o_k1), Encode(obj_buf, o_v1));

            // OrderLine Tuples
            // OrderLine(1, 1, 1, 1, 50.0)
            const order_line::key ol_k1;
            ol_k1.ol_w_id = 1;
            ol_k1.ol_d_id = 1;
            ol_k1.ol_o_id = 1;
            ol_k1.ol_number = 1;
            const order_line::value ol_v1;
            ol_v1.ol_delivery_d = 1;
            ol_v1.ol_amount = 50.0;
            open_tables["order_line"]->insert(txn, Encode(ol_k1), Encode(obj_buf, ol_v1));

            // OrderLine(1, 1, 1, 2, 25.0)
            const order_line::key ol_k2;
            ol_k2.ol_w_id = 1;
            ol_k2.ol_d_id = 1;
            ol_k2.ol_o_id = 1;
            ol_k2.ol_number = 2;
            const order_line::value ol_v2;
            ol_v2.ol_delivery_d = 1;
            ol_v2.ol_amount = 25.0;
            open_tables["order_line"]->insert(txn, Encode(ol_k2), Encode(obj_buf, ol_v2));

            ALWAYS_ASSERT(db->commit_txn(txn));
            arena.reset();

            if (verbose) {
                cerr << "[INFO] finished loading schedule data" << endl;
            }
        }
};

class schedule_worker : public bench_worker {
    public:
        schedule_worker(unsigned int worker_id, unsigned long seed, abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables,
                        spin_barrier *barrier_a, spin_barrier *barrier_b)
        : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b) {
            obj_key0.reserve(str_arena::MinStrReserveLength);
            obj_key1.reserve(str_arena::MinStrReserveLength);
            obj_v.reserve(str_arena::MinStrReserveLength);

            if (verbose) {
                cerr << "schedule workload: worker id " << worker_id
            }
        }

        virtual workload_desc_vec get_workload() const {
            workload_desc_vec w;

            if (worker_id == 1) {
                w.push_back(workload_desc("Payment1", 1.0, TxnPayment1));
            } else if (worker_id == 2) {
                w.push_back(workload_desc("Payment2", 1.0, TxnPayment2));
            } else if (worker_id == 3) {
                w.push_back(workload_desc("Delivery", 1.0, TxnDelivery));
            }

            return w;
        }

        static txn_result TxnPayment1(bench_worker *w) {
            return static_cast<schedule_worker *w>(w)->txn_payment1();

        }

        static txn_result TxnPayment2(bench_worker *w) {
            return static_cast<schedule_worker *w>(w)->txn_payment2();
        }

        static txn_result TxnDelivery(bench_worker *w) {
            return static_cast<schedule_worker *w>(w)->txn_delivery();
        }

        txn_result txn_payment1() {
            // TODO
            cerr << "executing payment 1" << endl;
            return txn_result(false, 0);
        }

        txn_result txn_payment2() {
            cerr << "executing payment 2" << endl;
            return txn_result(false, 0);
        }

        txn_result txn_delivery() {
            // TODO
            cerr << "executing delivery" << endl;
            return txn_result(false, 0);
        }
};

class schedule_bench_runner: public bench_runner {
    public:
        schedule_bench_runner(abstract_db *db) : bench_runner(db) {
            open_tables["warehouse"] = db->open_index("warehouse", size_of(warehouse));
            open_tables["district"] = db->open_index("district", size_of(district));
            open_tables["customer"] = db->open_index("customer", size_of(customer));
            open_tables["order"] = db->open_index("order", size_of(order));
            open_tables["order_line"] = db->open_index("order_line", size_of(order_line));
        }

    protected:
        virtual vector<bench_loader *> make_loaders() {
            vector<bench_loader *> ret;

            ret.push_back(new schedule_loader(1, db, open_tables));

            return ret;
        }

        virtual vector<bench_worker *> make_workers() {
            const unsigned alignment = coreid::num_cpus_online();
            const int blockstart = coreid::allocate_contiguous_aligned_block(nthreads, alignment);
            vector<bench_worker *> ret;

            ret.push_back(new schedule_worker(1, 1, db, open_tables, &barrier_a, &barrier_b));
            ret.push_back(new schedule_worker(2, 1, db, open_tables, &barrier_a, &barrier_b));
            ret.push_back(new schedule_worker(3, 1, db, open_tables, &barrier_a, &barrier_b));

            return ret;
        }
};

void schedule_do_test(abstract_db *db, int argc, char** argv) {
    schedule_bench_runner r(db);
    r.run();
}
