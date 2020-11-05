#ifndef ZEPHYR_IMPL_REGISTER_H_
#define ZEPHYR_IMPL_REGISTER_H_

#include <functional>
#include <memory>

namespace zephyr {
namespace grpc {

template <typename T>
using ImplCreator = std::function<std::unique_ptr<T>()>;

template <typename T>
class ImplFactory {
 public:
  static std::unique_ptr<T> New();
  static bool Register(ImplCreator<T> creator);

 private:
  static ImplCreator<T> &GetCreator();
};

template <typename T>
std::unique_ptr<T> ImplFactory<T>::New() {
  ImplCreator<T> &the_creator = GetCreator();
  if (the_creator) {
    return the_creator();
  } else {
    return nullptr;
  }
}

template <typename T>
bool ImplFactory<T>::Register(ImplCreator<T> creator) {
  GetCreator() = creator;
  return true;
}

template <typename T>
ImplCreator<T> &ImplFactory<T>::GetCreator() {
  static ImplCreator<T> the_creator;
  return the_creator;
}

#define REGISTER_IMPL(INTERFACE, IMPL)                   \
  static bool IMPL##_registerd __attribute__((unused)) ( \
      ImplFactory<INTERFACE>::Register([] {              \
        return std::unique_ptr<INTERFACE>(new IMPL);     \
      }));

}  // namespace grpc
}  // namespace zephyr


#endif  // ZEPHYR_IMPL_REGISTER_H_
