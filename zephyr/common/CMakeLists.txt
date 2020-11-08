file(GLOB_RECURSE com_files *.cpp *.h)
file(GLOB_RECURSE utils_files ../utils/*.cpp ../utils/*.h)

add_library(common STATIC ${com_files} ${utils_files})