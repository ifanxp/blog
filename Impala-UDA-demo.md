```c++
// uda-bitor.h
#ifndef BITOR_UDA_H
#define BITOR_UDA_H
#include <impala_udf/udf.h>

using namespace impala_udf;

void BitOrInit(FunctionContext* context, BigIntVal* val);
void BitOrUpdate(FunctionContext* context, const BigIntVal& input, BigIntVal* val);
void BitOrMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal BitOrFinalize(FunctionContext* context, const BigIntVal& val);
#endif

//uda-bitor.cc
#include "uda-bitor.h"
#include <assert.h>
#include <sstream>

void BitOrInit(FunctionContext* context, BigIntVal* val) {
  val->is_null = false;
  val->val = 0;
}

void BitOrUpdate(FunctionContext* context, const BigIntVal& input, BigIntVal* val) {
  if (input.is_null) return;
  val->val |= input.val;
}

void BitOrMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val |= src.val;
}

BigIntVal BitOrFinalize(FunctionContext* context, const BigIntVal& val) {
  return val;
}
```
