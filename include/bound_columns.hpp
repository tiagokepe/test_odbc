#include "duckdb.hpp"

#include<memory>
#include<map>
#include<vector>
#include <iostream>

namespace odbc_scanner {

#define ROW_PER_FETCH 10

class BoundColumns {
public:
	explicit BoundColumns() {};

private:
    struct Base {
        virtual ~Base() {}
        std::vector<SQLLEN> vec_ind_ptr;
    };

    template <typename T>
    struct Value: Base {
        std::vector<T> vec_value;
    };

	std::map<int, std::unique_ptr<Base>> col_values;

public:
	template <typename T>
    T *GetDataPtr(int idx)  {
        std::unique_ptr<Base>& val(col_values[idx]);
        bool need_allocation = false;
		if (!val) {
            val = duckdb::make_unique<Value<T>>();
            need_allocation = true;
		}
        auto col = static_cast<Value<T>&>(*val);
        if (need_allocation) {
            col.vec_value.resize(ROW_PER_FETCH);
            col.vec_ind_ptr.resize(ROW_PER_FETCH);
        }
		return col.vec_value.data();
    }

	template <typename T>
    std::vector<T>& GetVecValues(int idx)  {
        std::unique_ptr<Base>& rc(col_values[idx]);
        if (!rc) {
            rc = duckdb::make_unique<Value<T>>();
		}
        return static_cast<Value<T>&>(*rc).vec_value;
    }

    SQLLEN *GetIndicatorPtr(int idx)  {
        // std::unique_ptr<Base>& val(col_values[idx]);
        std::unique_ptr<Base>& val(col_values[idx]);
        if (val) {
            std::cout << val->vec_ind_ptr.capacity() << std::endl;
        }
        auto &col = col_values[idx];
		return col->vec_ind_ptr.data();
    }
};

} // namespace odbc_scanner