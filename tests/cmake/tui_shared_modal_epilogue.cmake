if(NOT DEFINED SOURCE)
  message(FATAL_ERROR "SOURCE is required")
endif()

file(READ "${SOURCE}" source)
string(REGEX MATCHALL
  "DrawModalEpilogue\\(rows, cols, command_error_open"
  epilogue_calls "${source}")
list(LENGTH epilogue_calls epilogue_call_count)
if(NOT epilogue_call_count EQUAL 1)
  message(FATAL_ERROR
    "DrawSummary must have exactly one shared modal epilogue call; found ${epilogue_call_count}")
endif()

string(FIND "${source}" "void DrawSummary(" draw_summary_begin)
string(FIND "${source}" "DrawFrameBody(run_root" frame_body_call)
string(FIND "${source}"
  "DrawModalEpilogue(rows, cols, command_error_open" epilogue_call)
if(draw_summary_begin EQUAL -1 OR frame_body_call EQUAL -1 OR
   epilogue_call EQUAL -1 OR NOT draw_summary_begin LESS frame_body_call OR
   NOT frame_body_call LESS epilogue_call)
  message(FATAL_ERROR
    "DrawSummary frame body must converge on its shared modal epilogue")
endif()
