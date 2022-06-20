#pragma once

#include "duckdb/common/types.hpp"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>
#include <vector>
#include <locale>

namespace odbc_scanner {

    struct OdbcConnection;

    struct OdbcColumnBind;

    struct OdbcScannerUtils {
    public:
        static void GetODBCDiagnosticMessages(std::string &msg, SQLSMALLINT htype, SQLHANDLE handle);

        static void CheckResult(SQLRETURN ret, SQLSMALLINT htype, SQLHANDLE handle, std::string msg = "");

        static void OdbcConnect(OdbcConnection &odbc_conn, std::string connecton_str);

        static duckdb::LogicalType GetLogicalType(SQLSMALLINT odbc_type);

        static SQLSMALLINT GetCDataType(SQLSMALLINT odbc_type);

        static duckdb::idx_t GetRowCount(OdbcConnection odbc_conn, std::string table_name);

        // static unique_ptr<OdbcColumnBind> GetOdbcColumnBind(SQLSMALLINT sql_type, duckdb::idx_t size);
    };

    struct OdbcConnection {
    public:
        OdbcConnection() {}
        OdbcConnection(const std::string &conn_str);
        ~OdbcConnection();
        void Init(const std::string &conn_str);

    public:
        SQLHENV henv = NULL;  // Environment handle
        SQLHDBC hconn = NULL; // Connection handle
    };                        // OdbcConnection

    struct OdbcStatement {
    public:
        OdbcStatement() {}
        OdbcStatement(OdbcConnection odbc_conn);
        ~OdbcStatement();
        void Init(OdbcConnection odbc_conn);
        void SetFetchArraySize(const duckdb::idx_t &rows_per_fetch);
        void SetColumnBindOrientation();

    public:
        SQLHSTMT hstmt = NULL; // Statement handle
    };

    struct CatalogBinding {
        SQLSMALLINT type = SQL_C_CHAR;
        SQLCHAR value_str[1024];
        SQLINTEGER buff_len = 1024;
        SQLLEN str_len_or_ind;
    };

    struct OdbcColumnBind {
    public:
        OdbcColumnBind() {}

        SQLINTEGER *GetIndicatorPtr(duckdb::idx_t idx) {
            return &ind_ptr[idx];
        }
        // default size a string to get from the ODBC driver
        #define DEFAULT_STR_SIZE 100
        std::vector<SQLINTEGER> ind_ptr;
    };

    template <typename VALUE_TYPE>
    struct OdbcColumnBindImpl : public OdbcColumnBind {
    public:
        explicit OdbcColumnBindImpl(duckdb::idx_t size) {
            ind_ptr.reserve(size);
            data_value.reserve(size);
        }
        VALUE_TYPE *GetData() {
            return data_value.data();
        }

    private:
        std::vector<VALUE_TYPE> data_value;
    };

    template <>
    struct OdbcColumnBindImpl<unsigned char *> : public OdbcColumnBind {
    public:
        OdbcColumnBindImpl(duckdb::idx_t size) {
            ind_ptr.reserve(size);
            data_value.reserve(size);
            for (duckdb::idx_t i=0; i < size; ++i) {
                data_value.emplace_back(new unsigned char[DEFAULT_STR_SIZE]);
            }        
        }
        unsigned char **GetData() {
            return data_value.data();
        }
        SQLINTEGER *GetIndicatorPtr(duckdb::idx_t idx) {
            return &ind_ptr[idx];
        }

    private:
        std::vector<unsigned char *> data_value;
    };
} // namespace odbc_scanner