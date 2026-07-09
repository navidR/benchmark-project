#pragma once

#include <yaml.h>

#include <cstddef>
#include <string>

namespace bsim {

class YamlEvent {
 public:
  YamlEvent() = default;
  ~YamlEvent();

  YamlEvent(const YamlEvent&) = delete;
  YamlEvent& operator=(const YamlEvent&) = delete;
  YamlEvent(YamlEvent&& other) noexcept;
  YamlEvent& operator=(YamlEvent&& other) noexcept;

  yaml_event_t* Mutable();
  void Activate();
  yaml_event_type_t Type() const;
  const yaml_event_t& Raw() const;

 private:
  yaml_event_t event_{};
  bool active_ = false;
};

class YamlParser {
 public:
  YamlParser(std::string input, std::string source);
  ~YamlParser();

  YamlParser(const YamlParser&) = delete;
  YamlParser& operator=(const YamlParser&) = delete;

  YamlEvent Next();

 private:
  std::string ErrorMessage() const;

  yaml_parser_t parser_{};
  std::string input_;
  std::string source_;
};

class YamlEmitter {
 public:
  YamlEmitter();
  ~YamlEmitter();

  YamlEmitter(const YamlEmitter&) = delete;
  YamlEmitter& operator=(const YamlEmitter&) = delete;

  void Emit(yaml_event_t* event);
  std::string Output() const;

 private:
  static int WriteHandler(void* data, unsigned char* buffer, std::size_t size);

  yaml_emitter_t emitter_{};
  std::string output_;
};

}  // namespace bsim
