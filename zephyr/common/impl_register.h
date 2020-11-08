#ifndef ZEPHYR_IMPL_REGISTER_H_
#define ZEPHYR_IMPL_REGISTER_H_

#include "zephyr/utils/imports.h"

namespace zephyr {
namespace common {

template <typename T> using ImplCreator = function<unique_ptr<T>()>;

template <typename T> class ImplFactory {
public:
  static unique_ptr<T> New();
  static bool Register(ImplCreator<T> creator);

private:
  static ImplCreator<T> &GetCreator();
};

template <typename T> unique_ptr<T> ImplFactory<T>::New() {
  ImplCreator<T> &the_creator = GetCreator();
  if (the_creator) {
    return the_creator();
  } else {
    return nullptr;
  }
}

template <typename T> bool ImplFactory<T>::Register(ImplCreator<T> creator) {
  GetCreator() = creator;
  return true;
}

template <typename T> ImplCreator<T> &ImplFactory<T>::GetCreator() {
  static ImplCreator<T> the_creator;
  return the_creator;
}

#define REGISTER_IMPL(INTERFACE, IMPL)                                         \
  static bool IMPL##_registerd                                                 \
      __attribute__((unused))(ImplFactory<INTERFACE>::Register(                \
          [] { return unique_ptr<INTERFACE>(new IMPL); }));

} // namespace common
} // namespace zephyr

#endif // ZEPHYR_IMPL_REGISTER_H_
