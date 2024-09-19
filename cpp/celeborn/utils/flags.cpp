#include <gflags/gflags.h>

// Used in utils/CelebornException.cpp
DEFINE_bool(
    celeborn_exception_user_stacktrace_enabled,
    false,
    "Enable the stacktrace for user type of CelebornException");

DEFINE_bool(
    celeborn_exception_system_stacktrace_enabled,
    true,
    "Enable the stacktrace for system type of CelebornException");

DEFINE_int32(
    celeborn_exception_user_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " user type of CelebornException; off when set to 0 (the default)");

DEFINE_int32(
    celeborn_exception_system_stacktrace_rate_limit_ms,
    0, // effectively turns off rate-limiting
    "Min time interval in milliseconds between stack traces captured in"
    " system type of CelebornException; off when set to 0 (the default)");

// Used in utils/ProcessBase.cpp

DEFINE_bool(celeborn_avx2, true, "Enables use of AVX2 when available");

DEFINE_bool(celeborn_bmi2, true, "Enables use of BMI2 when available");
