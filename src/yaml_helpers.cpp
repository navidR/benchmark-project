#include "bbp/simulator/yaml_helpers.h"

#include <stdexcept>
#include <utility>

namespace bbp {

YamlEvent::~YamlEvent() {
  if (active_) {
    yaml_event_delete(&event_);
  }
}

YamlEvent::YamlEvent(YamlEvent&& other) noexcept
    : event_(other.event_), active_(other.active_) {
  other.active_ = false;
}

YamlEvent& YamlEvent::operator=(YamlEvent&& other) noexcept {
  if (this != &other) {
    if (active_) {
      yaml_event_delete(&event_);
    }
    event_ = other.event_;
    active_ = other.active_;
    other.active_ = false;
  }
  return *this;
}

yaml_event_t* YamlEvent::Mutable() { return &event_; }

void YamlEvent::Activate() { active_ = true; }

yaml_event_type_t YamlEvent::Type() const { return event_.type; }

const yaml_event_t& YamlEvent::Raw() const { return event_; }

YamlParser::YamlParser(std::string input, std::string source)
    : input_(std::move(input)), source_(std::move(source)) {
  if (yaml_parser_initialize(&parser_) == 0) {
    throw std::runtime_error("yaml parser initialization failed");
  }
  yaml_parser_set_input_string(
      &parser_, reinterpret_cast<const unsigned char*>(input_.data()),
      input_.size());
}

YamlParser::~YamlParser() { yaml_parser_delete(&parser_); }

YamlEvent YamlParser::Next() {
  YamlEvent event;
  if (yaml_parser_parse(&parser_, event.Mutable()) == 0) {
    throw std::runtime_error(ErrorMessage());
  }
  event.Activate();
  return event;
}

std::string YamlParser::ErrorMessage() const {
  const char* problem = parser_.problem;
  std::string message = "yaml parse failed";
  if (!source_.empty()) {
    message += " in " + source_;
  }
  if (problem != nullptr) {
    message += ": ";
    message += problem;
  }
  message += " at line " + std::to_string(parser_.problem_mark.line + 1U);
  message += ", column " + std::to_string(parser_.problem_mark.column + 1U);
  return message;
}

YamlEmitter::YamlEmitter() {
  if (yaml_emitter_initialize(&emitter_) == 0) {
    throw std::runtime_error("yaml emitter initialization failed");
  }
  yaml_emitter_set_output(&emitter_, &YamlEmitter::WriteHandler, &output_);
  yaml_emitter_set_unicode(&emitter_, 1);
  yaml_emitter_set_indent(&emitter_, 2);
}

YamlEmitter::~YamlEmitter() { yaml_emitter_delete(&emitter_); }

void YamlEmitter::Emit(yaml_event_t* event) {
  if (yaml_emitter_emit(&emitter_, event) == 0) {
    const char* problem = emitter_.problem;
    throw std::runtime_error(std::string("yaml emit failed: ") +
                             (problem == nullptr ? "unknown error" : problem));
  }
}

std::string YamlEmitter::Output() const { return output_; }

int YamlEmitter::WriteHandler(void* data, unsigned char* buffer,
                              std::size_t size) {
  auto* output = static_cast<std::string*>(data);
  output->append(reinterpret_cast<const char*>(buffer), size);
  return 1;
}

}  // namespace bbp
