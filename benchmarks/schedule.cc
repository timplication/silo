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
            open_tables["order_line"]->insert(txn, "1,1,1,1", "1,50");
            // OrderLine(1, 1, 1, 2, 25.0)
            open_tables["order_line"]->insert(txn, "1,1,1,2", "1,25");

            ALWAYS_ASSERT(db->commit_txn(txn));
            arena.reset();

            if (verbose) {
                cerr << "[INFO] finished loading schedule data" << endl;
            }
        }
};

class schedule_worker : public bench_worker {
    private:
        string obj_key0;
        string obj_key1;
        string obj_v;

        unsigned int this_worker;
    public:
        schedule_worker(unsigned int worker_id, unsigned long seed, abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables,
                        spin_barrier *barrier_a, spin_barrier *barrier_b)
        : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b) {
            obj_key0.reserve(str_arena::MinStrReserveLength);
            obj_key1.reserve(str_arena::MinStrReserveLength);
            obj_v.reserve(str_arena::MinStrReserveLength);
            this_worker = worker_id;

            if (verbose) {
                cerr << "schedule workload: worker id " << worker_id << endl;
            }
        }

        virtual workload_desc_vec get_workload() const {
            workload_desc_vec w;

            if (this_worker == 1) {
                w.push_back(workload_desc("Payment1", 1.0, TxnPayment1));
            } else if (this_worker == 2) {
                w.push_back(workload_desc("Payment2", 1.0, TxnPayment2));
            } else if (this_worker == 3) {
                w.push_back(workload_desc("Delivery", 1.0, TxnDelivery));
            }

            return w;
        }

        static txn_result TxnPayment1(bench_worker *w) {
            return static_cast<schedule_worker *>(w)->txn_payment1();
        }

        static txn_result TxnPayment2(bench_worker *w) {
            return static_cast<schedule_worker *>(w)->txn_payment2();
        }

        static txn_result TxnDelivery(bench_worker *w) {
            return static_cast<schedule_worker *>(w)->txn_delivery();
        }

        txn_result txn_payment1() {
            cerr << "[WORKER " << this_worker << "] Starting TXN-Payment1" << endl;

            // Begin Transaction
            void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_PAYMENT);
            scoped_str_arena s_arena(arena);

            // Operation 1: R[t_1: Warehouse{1, W_YTD}]
            open_tables["warehouse"]->get(txn, "1", obj_v);
            uint w_ytd = stoi(obj_v);

            // Operation 2: W[t_1: Warehouse{W_YTD + 100}]
            open_tables["warehouse"]->put(txn, "1", to_string(w_ytd + 100));

            // Operation 3: R[t_2: District{1, 1, D_YTD}]
            open_tables["district"]->get(txn, "1,1", obj_v);
            uint d_ytd = stoi(obj_v);

            // Operation 4: W[t_2: District{D_YTD + 100}]
            open_tables["district"]->put(txn, "1,1", to_string(d_ytd + 100));

            // Operation 5: R[t_3: Customer{1, 1, 1, BAL}]
            open_tables["customer"]->get(txn, "1,1,1", obj_v);
            uint bal = stoi(bal);

            // Operation 6: W[t_3: Customer{BAL + 100}]
            open_tables["customer"]->put(txn, "1,1,1", to_string(bal + 100));

            // Commit Transaction
            cerr << "[WORKER " << this_worker << "] Committing TXN-Payment1..." << endl;
            if (likely(db->commit_txn(txn))) {
                cerr << "[WORKER " << this_worker << "] Committed TXN-Payment1 ✅" << endl;
                return txn_result(true, 0);
            }

            cerr << "[WORKER " << this_worker << "] Aborted TXN-Payment1 ⛔️" << endl;
            return txn_result(false, 0);
        }

        txn_result txn_payment2() {
            cerr << "[WORKER " << this_worker << "] Starting TXN-Payment2" << endl;

            // Begin Transaction
            void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_PAYMENT);
            scoped_str_arena s_arena(arena);

            // Operation 1: R[t_7: Warehouse{2, W_YTD}]
            open_tables["warehouse"]->get(txn, "2", obj_v);
            uint w_ytd = stoi(obj_v);

            // Operation 2: W[t_7: Warehouse{W_YTD + 200}]
            open_tables["warehouse"]->put(txn, "2", to_string(w_ytd + 200));

            // Operation 3: R[t_8: District{2, 2, D_YTD}]
            open_tables["district"]->get(txn, "2,2", obj_v);
            uint d_ytd = stoi(obj_v);

            // Operation 4: W[t_8: District{D_YTD + 200}]
            open_tables["district"]->put(txn, "2,2", to_string(d_ytd + 200));

            // Operation 5: R[t_9: Customer{2, 2, 2, BAL}]
            open_tables["customer"]->get(txn, "2,2,2", obj_v);
            uint bal = stoi(bal);

            // Operation 6: W[t_9: Customer{BAL + 200}]
            open_tables["customer"]->put(txn, "2,2,2", to_string(bal + 200));

            // Commit Transaction
            cerr << "[WORKER " << this_worker << "] Committing TXN-Payment2..." << endl;
            if (likely(db->commit_txn(txn))) {
                cerr << "[WORKER " << this_worker << "] Committed TXN-Payment2 ✅" << endl;
                return txn_result(true, 0);
            }

            cerr << "[WORKER " << this_worker << "] Aborted TXN-Payment2 ⛔️" << endl;
            return txn_result(false, 0);
        }

        txn_result txn_delivery() {
            cerr << "[WORKER " << this_worker << "] Starting TXN-Delivery" << endl;

            // Begin Transaction
            void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCC_PAYMENT);
            scoped_str_arena s_arena(arena);

            // R[t_4: Order{1, 1, 1, C_ID, CA_ID}]
            open_tables["order"]->get(txn, "1,1,1", obj_v);
            uint c_id = stoi(obj_v.substr(0, obj_v.find(',')));

            // W[t_4: Order{CA_ID = 2}]
            open_tables["order"]->put(txn, "1,1,1", to_string(c_id) + ",2");

            // R[t_5: OrderLine{1, 1, 1, 1, DEL_1, AMT_1}]
            open_tables["order_line"]->get(txn, "1,1,1,1", obj_v);
            size_t idx = obj_v.find(',');
            uint del_1 = stoi(obj_v.substr(0, idx));
            uint amt_1 = stoi(obj_v.substr(idx+1));

            // W[t_5: OrderLine{DEL_1}]
            open_tables["order_line"]->put(txn, "1,1,1,1",
                to_string(del_1 + 1) + "," + to_string(amt_1));

            // R[t_6: OrderLine{1, 1, 1, 2, DEL_2, AMT_2}]
            open_tables["order_line"]->get(txn, "1,1,1,2", obj_v);
            size_t idx = obj_v.find(',');
            uint del_2 = stoi(obj_v.substr(0, idx));
            uint amt_2 = stoi(obj_v.substr(idx+1));

            // W[t_6: OrderLine{DEL_2}]
            open_tables["order_line"]->put(txn, "1,1,1,2",
                to_string(del_2 + 1) + "," + to_string(amt_2));

            // R[t_3: Customer{1, 1, C_ID, BAL}]
            open_tables["customer"]->get(txn, "1,1," + to_string(c_id), obj_v);
            uint bal = stoi(obj_v);

            // W[t_3: Customer{BAL - (AMT_1 + AMT_2)}]
            open_tables["customer"]->put(txn, "1,1," + to_string(c_id), to_string(bal - (amt_1 + amt_2)));

            // Commit Transaction
            cerr << "[WORKER " << this_worker << "] Committing TXN-Delivery..." << endl;
            if (likely(db->commit_txn(txn))) {
                cerr << "[WORKER " << this_worker << "] Committed TXN-Delivery ✅" << endl;
                return txn_result(true, 0);
            }

            cerr << "[WORKER " << this_worker << "] Aborted TXN-Delivery ⛔️" << endl;
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
