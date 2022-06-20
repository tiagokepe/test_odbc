#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "include/odbc_scanner_utils.hpp"
#include "include/bound_columns.hpp"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

using namespace std;
using std::string;

using namespace duckdb;
using odbc_scanner::CatalogBinding;
using odbc_scanner::OdbcConnection;
using odbc_scanner::OdbcColumnBind;
using odbc_scanner::OdbcColumnBindImpl;
using odbc_scanner::OdbcScannerUtils;
using odbc_scanner::OdbcStatement;
using odbc_scanner::BoundColumns;

#define STR_LEN 128 + 1

struct OdbcBindData : public FunctionData {
    string conn_str;
    string table_name;

    vector<string> names;
    vector<LogicalType> types;
    vector<SQLSMALLINT> odbc_sql_types;
    vector<SQLSMALLINT> odbc_c_types;

    idx_t max_rowid = 0;
    vector<bool> not_nulls;
    vector<uint64_t> decimal_multipliers;

    idx_t rows_per_group = 100000;
    idx_t rows_per_fetch = 10;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_unique<OdbcBindData>();
        copy->conn_str = conn_str;
        copy->table_name = table_name;
        copy->names = names;
        copy->types = types;
        copy->odbc_sql_types = odbc_sql_types;
        copy->max_rowid = max_rowid;
        copy->not_nulls = not_nulls;
        copy->decimal_multipliers = decimal_multipliers;
        copy->rows_per_group = rows_per_group;
        copy->rows_per_fetch = rows_per_fetch;

        return copy;
    }

    bool Equals(const FunctionData &other_p) const override {
        auto other = (OdbcBindData &)other_p;
        return other.conn_str == conn_str && other.table_name == table_name &&
               other.names == names && other.types == types && other.odbc_sql_types == odbc_sql_types &&
               other.max_rowid == max_rowid && other.not_nulls == not_nulls &&
               other.decimal_multipliers == decimal_multipliers &&
               other.rows_per_group == rows_per_group &&
               other.rows_per_fetch == rows_per_fetch;
    }
};

struct OdbcGlobalState : public GlobalTableFunctionState {
    OdbcGlobalState() : current_idx(0) {
    }
    idx_t current_idx;
};

static unique_ptr<GlobalTableFunctionState> OdbcFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_unique<OdbcGlobalState>();
}

static unique_ptr<FunctionData> OdbcBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {

    auto result = make_unique<OdbcBindData>();
    result->conn_str = input.inputs[0].GetValue<string>();
    result->table_name = input.inputs[1].GetValue<string>();

    OdbcConnection odbc_conn(result->conn_str);
    OdbcStatement odbc_stmt(odbc_conn);

    // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function?view=sql-server-ver16#:~:text=DATA_TYPE%20(ODBC%201.0,and%20Standards%20Compliance.
    auto rc = SQLColumns(odbc_stmt.hstmt, NULL, 0, NULL, 0, (SQLCHAR *)result->table_name.c_str(), SQL_NTS, NULL, 0);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, odbc_stmt.hstmt);

    SQLCHAR col_name[STR_LEN];
    SQLLEN len_col_name;

    SQLSMALLINT data_type;
    SQLLEN len_data_type;

    SQLSMALLINT nullable;
    SQLLEN len_nullable;

    SQLLEN cbColumnSize;
    SQLLEN cbDecimalDigits;
    SQLLEN cbNumPrecRadix;
    SQLLEN cbSQLDataType;
    SQLLEN cbDatetimeSubtypeCode;
    SQLLEN cbOrdinalPosition;

    SQLINTEGER ColumnSize;
    SQLINTEGER OrdinalPosition;

    SQLSMALLINT DecimalDigits;
    SQLSMALLINT NumPrecRadix;
    SQLSMALLINT SQLDataType;
    SQLSMALLINT DatetimeSubtypeCode;

    // Bind columns in result set to buffers
    SQLBindCol(odbc_stmt.hstmt, 4, SQL_C_CHAR, col_name, STR_LEN, &len_col_name);
    SQLBindCol(odbc_stmt.hstmt, 5, SQL_C_SSHORT, &data_type, 0, &len_data_type);
    SQLBindCol(odbc_stmt.hstmt, 7, SQL_C_SLONG, &ColumnSize, 0, &cbColumnSize);
    SQLBindCol(odbc_stmt.hstmt, 9, SQL_C_SSHORT, &DecimalDigits, 0, &cbDecimalDigits);
    SQLBindCol(odbc_stmt.hstmt, 10, SQL_C_SSHORT, &NumPrecRadix, 0, &cbNumPrecRadix);
    SQLBindCol(odbc_stmt.hstmt, 11, SQL_C_SSHORT, &nullable, 0, &len_nullable);
    SQLBindCol(odbc_stmt.hstmt, 14, SQL_C_SSHORT, &SQLDataType, 0, &cbSQLDataType);
    SQLBindCol(odbc_stmt.hstmt, 15, SQL_C_SSHORT, &DatetimeSubtypeCode, 0, &cbDatetimeSubtypeCode);
    SQLBindCol(odbc_stmt.hstmt, 17, SQL_C_SLONG, &OrdinalPosition, 0, &cbOrdinalPosition);

    while (SQLFetch(odbc_stmt.hstmt) == SQL_SUCCESS) {
        names.emplace_back(std::string((const char *)col_name));

        LogicalType logic_type = OdbcScannerUtils::GetLogicalType(data_type);
        return_types.push_back(logic_type);
        result->odbc_sql_types.emplace_back(data_type);

        bool not_null = (bool)nullable;
        result->not_nulls.emplace_back(not_null);

        result->odbc_c_types.emplace_back(OdbcScannerUtils::GetCDataType(data_type));
    }

    result->max_rowid = OdbcScannerUtils::GetRowCount(odbc_conn, result->table_name);

    result->names = names;
    result->types = return_types;

    return move(result);
}

static unique_ptr<NodeStatistics> OdbcCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);

    auto bind_data = (const OdbcBindData *)bind_data_p;
    return make_unique<NodeStatistics>(bind_data->max_rowid);
}

static void OdbcScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto bind_data = (const OdbcBindData *)data.bind_data;
    OdbcConnection odbc_conn(bind_data->conn_str);
    OdbcStatement odbc_stmt(odbc_conn);
    odbc_stmt.SetFetchArraySize(bind_data->rows_per_fetch);
    odbc_stmt.SetColumnBindOrientation();

    auto num_cols = output.ColumnCount();
    // vector<unique_ptr<OdbcColumnBind>> bound_columns;
    // for (idx_t col_idx = 0; col_idx < num_cols; col_idx++) {
    //     switch (bind_data->odbc_sql_types[col_idx]) {
    //     case SQL_INTEGER:
    //         bound_columns.emplace_back(make_unique<OdbcColumnBindImpl<SQLINTEGER>>(bind_data->rows_per_fetch));
    //         break;
    //     case SQL_DECIMAL:
    //         bound_columns.emplace_back(make_unique<OdbcColumnBindImpl<SQLDOUBLE>>(bind_data->rows_per_fetch));
    //         break;
    //     case SQL_CHAR:
    //         bound_columns.emplace_back(make_unique<OdbcColumnBindImpl<SQLCHAR>>(bind_data->rows_per_fetch));
    //         break;
    //     case SQL_VARCHAR:
    //     default:
    //         bound_columns.emplace_back(make_unique<OdbcColumnBindImpl<SQLCHAR *>>(bind_data->rows_per_fetch));
    //         break;
    //     }
    // }
    BoundColumns bound_cols;
    for (idx_t col_idx = 0; col_idx < num_cols; col_idx++) {
        switch (bind_data->odbc_sql_types[col_idx]) {
        case SQL_INTEGER: {
            auto odbc_c_type = bind_data->odbc_c_types[col_idx];
            auto col_val_ptr = bound_cols.GetDataPtr<SQLINTEGER>(col_idx);
            auto col_ind_ptr = bound_cols.GetIndicatorPtr(col_idx);
            auto col_val_ptr2 = bound_cols.GetDataPtr<SQLINTEGER>(col_idx);
            if(col_val_ptr2){
                ;
            }

            auto vec_values = bound_cols.GetVecValues<SQLINTEGER>(col_idx);
            if (vec_values.empty()) {
                ;
            }


            auto rc = SQLBindCol(odbc_stmt.hstmt, 1, odbc_c_type, col_val_ptr, 0, col_ind_ptr);
            OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_DBC, odbc_conn.hconn, "SQLBindCol failed");
            break;
        }
        case SQL_DECIMAL:
            bound_cols.GetDataPtr<SQLDOUBLE>(col_idx);
            break;
        case SQL_CHAR:
            bound_cols.GetDataPtr<SQLCHAR>(col_idx);
            break;
        case SQL_VARCHAR:
        default:
            bound_cols.GetDataPtr<SQLCHAR *>(col_idx);
            break;
        }
    }

    // How to bind COL?
    for (idx_t col_idx = 0; col_idx < num_cols; col_idx++) {
        // auto odbc_c_type = bind_data->odbc_c_types[col_idx];
        // auto col_val_ptr = bound_columns[col_idx].get();
        // auto data = ((OdbcColumnBindImpl<> *)col_val_ptr)->GetData();
        // SQLBindCol(odbc_stmt.hstmt, 1, odbc_c_type, OrderIDArray, 0, OrderIDIndArray);
    }

    // auto sql = StringUtil::Format("SELECT * FROM \"%s\"", bind_data->table_name);

    while (SQLFetch(odbc_stmt.hstmt) == SQL_SUCCESS) {
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {

        }
    }
}

static string OdbcToString(const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);
    auto bind_data = (const OdbcBindData *)bind_data_p;
    return StringUtil::Format("%s:%s", bind_data->conn_str,
                              bind_data->table_name);
}

class OdbcScanFunction : public TableFunction {
public:
    OdbcScanFunction()
        : TableFunction("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, OdbcScan, OdbcBind,
                        OdbcFunctionInit) {
        cardinality = OdbcCardinality;
        to_string = OdbcToString;
        projection_pushdown = true;
    }
};

struct AttachFunctionData : public TableFunctionData
{
    AttachFunctionData() {}

    bool finished = false;
    bool overwrite = false;
    string connecton_str = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context,
                                           TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types,
                                           vector<string> &names) {

    auto result = make_unique<AttachFunctionData>();
    result->connecton_str = input.inputs[0].GetValue<string>();

    for (auto &kv : input.named_parameters)
    {
        if (kv.first == "overwrite")
        {
            result->overwrite = BooleanValue::Get(kv.second);
        }
    }

    return_types.push_back(LogicalType::BOOLEAN);
    names.emplace_back("Success");
    return move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (AttachFunctionData &)*data_p.bind_data;
    if (data.finished) {
        return;
    }

    auto dconn = Connection(context.db->GetDatabase(context));

    OdbcConnection odbc_conn(data.connecton_str);
    OdbcStatement odbc_stmt(odbc_conn);
    // OdbcScannerUtils::OdbcConnect(odbc_conn, data.connecton_str);
    // OdbcScannerUtils::OdbcSetStmtHandle(odbc_conn);

    SQLLEN ind_table_value;
    char table_name[1024];

    auto rc = SQLBindCol(odbc_stmt.hstmt, 3, SQL_C_CHAR, &table_name, sizeof(table_name), &ind_table_value);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, odbc_stmt.hstmt, "SQLBindCol failed.");

    // rc = SQLTables(odbc_conn.stmt, NULL, 0, (SQLCHAR *)"main", SQL_NTS, (SQLCHAR *)"%", SQL_NTS, (SQLCHAR *)"TABLE,VIEW", SQL_NTS);
    rc = SQLTables(odbc_stmt.hstmt, NULL, 0, NULL, 0, (SQLCHAR *)"%", SQL_NTS, (SQLCHAR *)"TABLE", SQL_NTS);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, odbc_stmt.hstmt);

    while (SQLFetch(odbc_stmt.hstmt) == SQL_SUCCESS) {
        // if (ind_table_value != SQL_NULL_DATA) {
        //   printf("Catalog Name = %s\n", table_name);
        // }
        auto scan_res = dconn.TableFunction("odbc_scan",
                                            {Value(data.connecton_str), Value(table_name)});

        scan_res->CreateView(table_name, data.overwrite, false);
    }

    data.finished = true;
}

extern "C" {
    DUCKDB_EXTENSION_API void odbc_scanner_init(duckdb::DatabaseInstance &db) {
        Connection con(db);
        con.BeginTransaction();
        auto &context = *con.context;
        auto &catalog = Catalog::GetCatalog(context);

        OdbcScanFunction odbc_fun;
        CreateTableFunctionInfo odbc_info(odbc_fun);
        catalog.CreateTableFunction(context, &odbc_info);

        TableFunction attach_func("odbc_attach", {LogicalType::VARCHAR},
                                  AttachFunction, AttachBind);
        attach_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;

        CreateTableFunctionInfo attach_info(attach_func);
        catalog.CreateTableFunction(context, &attach_info);

        con.Commit();
    }

    DUCKDB_EXTENSION_API const char *odbc_scanner_version() {
        return DuckDB::LibraryVersion();
    }
}
