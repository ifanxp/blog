```C++
#ifndef SAMPLES_UDF_H
#define SAMPLES_UDF_H

#include <impala_udf/udf.h>
using namespace impala_udf;

IntVal GetProvinceID(FunctionContext* context, const StringVal& arg1, const StringVal& arg2);

#endif
```


```C++
#include "udf-GetProvinceID.h"

#include <string>

// In this sample we are declaring a UDF that adds two ints and returns an int.
IntVal GetProvinceID(FunctionContext* context, const StringVal& arg1, const StringVal& arg2) {
  if (arg1.is_null || arg2.is_null) return IntVal::null();
  std::string str1((const char *)arg1.ptr,arg1.len);
  std::string str2((const char *)arg2.ptr,arg2.len);
  return IntVal(str1.compare(str2));
}
```
