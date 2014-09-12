#include "LogServerRequest.h"

namespace sf1r
{

const LogServerRequest::method_t LogServerRequest::method_names[] =
        {
            "test",
            
            "insert",
            "insert_with_values",
            "get_current_topk",
            "get_topk",
            "get_current_dvc",
            "get_dvc",
            "get_values",
            "get_value_and_count",
            "get_all_collection",
            "insert_prop_label",
            "get_prop_label",
            "del_prop_label"
        };

}
