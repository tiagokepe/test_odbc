#include "odbc_scanner_utils.hpp"

#include "duckdb/common/helper.hpp"

#include <sql.h>
#include <sqlext.h>
#include <stdexcept>
#include <memory>

using odbc_scanner::OdbcColumnBind;
using odbc_scanner::OdbcColumnBindImpl;
using odbc_scanner::OdbcConnection;
using odbc_scanner::OdbcScannerUtils;
using odbc_scanner::OdbcStatement;
using std::unique_ptr;

void OdbcScannerUtils::GetODBCDiagnosticMessages(std::string &msg, SQLSMALLINT htype, SQLHANDLE handle) {
    char sqlstate[32];
    char message[1000];
    SQLINTEGER native_error;
    SQLSMALLINT text_len;
    SQLRETURN ret;
    SQLSMALLINT recno = 0;

    do
    {
        recno++;
        ret = SQLGetDiagRec(htype, handle, recno, (SQLCHAR *)sqlstate, &native_error,
                            (SQLCHAR *)message, sizeof(message) - 1, &text_len);
        if (ret == SQL_INVALID_HANDLE)
        {
            msg += ("\nInvalid handle\n");
        }
        else if (SQL_SUCCEEDED(ret))
        {
            msg += "\n" + std::string(sqlstate) + "=" + message + "\n";
        }

    } while (SQL_SUCCEEDED(ret));
}

void OdbcScannerUtils::CheckResult(SQLRETURN ret, SQLSMALLINT htype, SQLHANDLE handle, std::string msg) {
    if (!SQL_SUCCEEDED(ret))
    {
        std::string error_message(msg);
        GetODBCDiagnosticMessages(error_message, htype, handle);
        throw std::runtime_error(error_message);
    }
}

void OdbcScannerUtils::OdbcConnect(OdbcConnection &odbc_conn, std::string connecton_str) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &odbc_conn.henv);
    SQLSetEnvAttr(odbc_conn.henv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);

    SQLAllocHandle(SQL_HANDLE_DBC, odbc_conn.henv, &odbc_conn.hconn);

    SQLCHAR out_conn_str[1024];
    SQLSMALLINT out_len;
    auto rc = SQLDriverConnect(odbc_conn.hconn, NULL, (SQLCHAR *)connecton_str.c_str(), SQL_NTS, out_conn_str, sizeof(out_conn_str), &out_len, SQL_DRIVER_COMPLETE);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_DBC, odbc_conn.hconn, "SQLConnect failed");
}

duckdb::LogicalType OdbcScannerUtils::GetLogicalType(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_INTEGER:
        return duckdb::LogicalType::INTEGER;
    case SQL_DECIMAL:
        return duckdb::LogicalType::DECIMAL(18, 3);
    case SQL_CHAR:
    case SQL_WVARCHAR:
        return duckdb::LogicalType::VARCHAR;
    default:
        return duckdb::LogicalType::VARCHAR;
    }
}

SQLSMALLINT OdbcScannerUtils::GetCDataType(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_INTEGER:
        return SQL_C_SLONG;
    case SQL_DECIMAL:
        return SQL_C_DOUBLE;
    case SQL_CHAR:
    default:
        return SQL_C_CHAR;
    }
}

duckdb::idx_t OdbcScannerUtils::GetRowCount(OdbcConnection odbc_conn, std::string table_name) {
    OdbcStatement odbc_stmt(odbc_conn);
    std::string sql = "SELECT COUNT(*) FROM " + table_name;
    SQLExecDirect(odbc_stmt.hstmt, (SQLCHAR *)sql.c_str(), (SQLINTEGER)sql.size());
    auto rc = SQLFetch(odbc_stmt.hstmt);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, odbc_stmt.hstmt);

    duckdb::idx_t count = 0;
    //SQLLEN count_len;
    SQLGetData(odbc_stmt.hstmt, 1, SQL_C_ULONG, &count, 0, NULL);

    return count;
}


// unique_ptr<OdbcColumnBind> OdbcScannerUtils::GetOdbcColumnBind(SQLSMALLINT sql_type, duckdb::idx_t size) {
//     if (sql_type == SQL_C_ULONG) {
//         auto bind_col = make_unique<OdbcColumnBindImpl<SQLUINTEGER>>(size);
//         return bind_col;
//     }

//     auto bind_col = make_unique<OdbcColumnBindImpl<SQLCHAR>>(size);
//     return bind_col;
// }


/*** OdbcConnection **********************************************************/

OdbcConnection::OdbcConnection(const std::string &conn_str) {
    Init(conn_str);
}

OdbcConnection::~OdbcConnection() {
    if (henv) {
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
    }
    if (hconn) {
      SQLFreeHandle(SQL_HANDLE_DBC, hconn);
    }
}

void OdbcConnection::Init(const std::string &conn_str) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);

    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);

    SQLCHAR out_conn_str[1024];
    SQLSMALLINT out_len;
    auto rc = SQLDriverConnect(hconn, NULL, (SQLCHAR *)conn_str.c_str(), SQL_NTS, out_conn_str, sizeof(out_conn_str), &out_len, SQL_DRIVER_COMPLETE);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_DBC, hconn, "SQLConnect failed");
}


/*** OdbcStatement ***********************************************************/

OdbcStatement::OdbcStatement(OdbcConnection odbc_conn) {
    Init(odbc_conn);
}

OdbcStatement::~OdbcStatement() {
    if (hstmt) {
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    }
}

void OdbcStatement::Init(OdbcConnection odbc_conn) {
    auto rc = SQLAllocHandle(SQL_HANDLE_STMT, odbc_conn.hconn, &hstmt);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, hstmt, "SQLAlloc statement failed.");

    rc = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER) 1, 0);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, hstmt, "SQLSetStmtAttr (SQL_ATTR_ROW_ARRAY_SIZE) failed.");
}

void OdbcStatement::SetFetchArraySize(const duckdb::idx_t &rows_per_fetch) {
    auto rc = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLULEN *)rows_per_fetch, 0);  
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, hstmt, "SQLSetStmtAttr failed to set the SQL_ATTR_ROW_ARRAY_SIZE.");
}

void OdbcStatement::SetColumnBindOrientation() {
    auto rc = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    OdbcScannerUtils::CheckResult(rc, SQL_HANDLE_STMT, hstmt, "SQLSetStmtAttr failed to set the SQL_ATTR_ROW_BIND_TYPE.");
}