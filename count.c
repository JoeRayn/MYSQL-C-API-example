/*
This program recalulates all SNV counts by reading the detection table from unidb
 */
#include <mysql/mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define NCOLUMNS 5


void cleanup(MYSQL *con, MYSQL_STMT *stmt);

MYSQL *setup_db_connection(void);

int bind_buffers(MYSQL_STMT *stmt);


int main(void)
{
  fprintf(stderr, "MySQL client version: %s\n", mysql_get_client_info());
  
  MYSQL *con = setup_db_connection();

  char *query_string = "select * from samples";
   
  // prepare select query 
  MYSQL_STMT *stmt = mysql_stmt_init(con);
  if (stmt==NULL) {
    fprintf(stderr, "mysql_stmt_init() failed (insufficent memory)\n");
    mysql_close(con);
    exit(1);
  }

  // maybe should cast the strlen to a ulong in myslq_stmt_prepare
  if (mysql_stmt_prepare(stmt, query_string, strlen(query_string))!=0) { 
    fprintf(stderr, "failed to prepare statement: %s\n", mysql_stmt_error(stmt));
    cleanup(con,stmt);
    exit(1);
  }
  fprintf(stderr, "Preparing query successful\n");

  // Get query meta data 
  MYSQL_RES *prepare_meta_result;
  prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result)
    {
      fprintf(stderr,
         " mysql_stmt_result_metadata(), \
           returned no meta information\n");
      fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
      cleanup(con,stmt);
      exit(1);
    }
 
  /* Get total columns in the query */
  int column_count;
  column_count= mysql_num_fields(prepare_meta_result);
  fprintf(stderr,
          "total columns in SELECT statement: %d\n",
          column_count);  
  if (column_count != NCOLUMNS) /* validate column count */
    {
      fprintf(stderr, "Invalid column count returned by MySQL\n");
      mysql_free_result(prepare_meta_result);
      cleanup(con,stmt);
      exit(1);
    }

  // Execute query
  if (mysql_stmt_execute(stmt) !=0) {
    fprintf(stderr, "failed to execute statement: %s\n", mysql_stmt_error(stmt));
    mysql_free_result(prepare_meta_result);
    cleanup(con,stmt);
    exit(1);
  }

  
  if (bind_buffers(stmt)){
      fprintf(stderr, "bind_buffers() failed\n");
      fprintf(stderr, "%s\n", mysql_stmt_error(stmt));
      mysql_free_result(prepare_meta_result);
      cleanup(con,stmt);
      exit(1);
    }

  int row_fetch_status, row_count; // is row count big enough? maybe a unsigned long is better
  while (1)
    {
      row_count++;
      row_fetch_status = mysql_stmt_fetch(stmt);
      if (row_fetch_status == 0){
        ;
      }
      else if (row_fetch_status == 1){
        fprintf(stderr, "failed to fetch row: %u, %s\n",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
        mysql_free_result(prepare_meta_result);
        cleanup(con,stmt);
        exit(1);
      }
      else if (row_fetch_status == MYSQL_DATA_TRUNCATED){
        fprintf(stderr, "Data truncated when fetching row: %u, %s\n",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
        mysql_free_result(prepare_meta_result);
        cleanup(con,stmt);
        exit(1);
      }
      else if (row_fetch_status == MYSQL_NO_DATA){
        fprintf(stderr, "finished fetching rows");
        break;
      }
      else {
        fprintf(stderr, "unexpected return type when fetching rows: %u, %s\n",
                mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
        mysql_free_result(prepare_meta_result);
        cleanup(con,stmt);
        exit(1);
      }
      
        
      // handle row here

    }
n
  mysql_free_result(prepare_meta_result);  
  cleanup(con, stmt);
  exit(0);
}

void cleanup(MYSQL *con, MYSQL_STMT *stmt){
  if (mysql_stmt_close(stmt)!=0){
    fprintf(stderr, "Could not close stmt: %u, %s\n",mysql_errno(con), mysql_error(con));
  }
  mysql_close(con);
}


MYSQL *setup_db_connection(void){
    MYSQL *con;
  con = mysql_init(NULL);

  const char* password = getenv("MYSQL_PASSWORD");
  if  (password==NULL) {
    fprintf(stderr, "MYSQL_PASSWORD environment varible is not set\n");
    exit(1);
  }
  
  const char* user = getenv("MYSQL_USER");
    if  (user==NULL) {
    fprintf(stderr, "MYSQL_USER environment varible is not set\n");
    exit(1);
  }

  if (con == NULL)
  {
      fprintf(stderr, "mysql_init() failed (insufficent memory)\n");
      exit(1);
  }
  
  if (mysql_real_connect(con, "localhost", user, password, "unidb2", 0, NULL, 0) == NULL) 
  {
      fprintf(stderr, "%s\n", mysql_error(con));
      mysql_close(con);
      exit(1);
  }
  return con;
}

int bind_buffers(MYSQL_STMT *stmt){
  /* Bind the result buffers for all columns before fetching them */
  //these variables should be passed as perameters to this function once I get my query right
  unsigned long length[NCOLUMNS];
  MYSQL_BIND bind[NCOLUMNS];
  short small_data;
  int int_data;
  char str_data[50];
  my_bool is_null[NCOLUMNS];
  my_bool error[NCOLUMNS];
  
  memset(bind, 0, sizeof(bind));
  
  bind[0].buffer_type= MYSQL_TYPE_LONG;
  bind[0].buffer= (char *)&int_data;
  bind[0].is_null= &is_null[0];
  bind[0].length= &length[0]; // ignored for numeric types
  bind[0].error= &error[0];
  bind[0].is_unsigned = 1; // this heed to match the variable

  bind[2].buffer_type= MYSQL_TYPE_SHORT;
  bind[2].buffer= (char *)&small_data;
  bind[2].is_null= &is_null[2];
  bind[2].length= &length[2];
  bind[2].error= &error[2];

  /* Bind the result buffers */
  return mysql_stmt_bind_result(stmt, bind);
}
