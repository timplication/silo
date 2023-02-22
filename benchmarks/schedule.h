#ifndef _NDB_BENCH_SCHEDULE_H_
#define _NDB_BENCH_SCHEDULE_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"

#define WAREHOUSE_KEY_FIELDS(x, y) \
  x(int32_t,w_id)
#define WAREHOUSE_VALUE_FIELDS(x, y) \
  x(float,w_ytd) \
DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS)

#define DISTRICT_KEY_FIELDS(x, y) \
  x(int32_t,d_w_id) \
  y(int32_t,d_id)
#define DISTRICT_VALUE_FIELDS(x, y) \
  x(float,d_ytd) \
DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS)

#define CUSTOMER_KEY_FIELDS(x, y) \
  x(int32_t,c_w_id) \
  y(int32_t,c_d_id) \
  y(int32_t,c_id)
#define CUSTOMER_VALUE_FIELDS(x, y) \
  y(float,c_balance) \
DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS)

#define ORDER_KEY_FIELDS(x, y) \
  x(int32_t,o_w_id) \
  y(int32_t,o_d_id) \
  y(int32_t,o_id)
#define ORDER_VALUE_FIELDS(x, y) \
  x(int32_t,o_c_id) \
  y(int32_t,o_carrier_id) \
DO_STRUCT(order, ORDER_KEY_FIELDS, ORDER_VALUE_FIELDS)

#define ORDER_LINE_KEY_FIELDS(x, y) \
  x(int32_t,ol_w_id) \
  y(int32_t,ol_d_id) \
  y(int32_t,ol_o_id) \
  y(int32_t,ol_number)
#define ORDER_LINE_VALUE_FIELDS(x, y) \
  y(uint32_t,ol_delivery_d) \
  y(float,ol_amount) \
DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS)

#endif
