#include <iostream>
#include <sstream>
#include <utility>
#include <vector>
#include <map>
#include <string>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../core.h"

#include "bench.h"

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

            // Warehouse(1, 8000000)
            open_tables["warehouse"]->insert(txn, "1", "8000000");
            // Warehouse(2, 1600000)
            open_tables["warehouse"]->insert(txn, "2", "1600000");
            // District(1, 1, 8000000)
            open_tables["district"]->insert(txn, "1,1", "8000000");
            // District(2, 2, 1600000)
            open_tables["district"]->insert(txn, "2,2", "1600000");
            // Customer(1, 1, 1, 1000)
            open_tables["customer"]->insert(txn, "1,1,1", "1000");
            // Customer(2, 2, 2, 5000)
            open_tables["customer"]->insert(txn, "2,2,2", "5000");
            // Order(1, 1, 1, 1, 1)
            open_tables["order"]->insert(txn, "1,1,1", "1,1");
            // OrderLine(1, 1, 1, 1, 50.0)
            open_tables["order_line"]->insert(txn, "1,1,1,1", "1,50.0");
            // OrderLine(1, 1, 1, 2, 25.0)
            open_tables["order_line"]->insert(txn, "1,1,1,2", "1,25.0");

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
            open_tables["warehouse"] = db->open_index("warehouse", 128);
            open_tables["district"] = db->open_index("district", 128);
            open_tables["customer"] = db->open_index("customer", 128);
            open_tables["order"] = db->open_index("order", 128);
            open_tables["order_line"] = db->open_index("order_line", 128);
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
