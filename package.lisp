
(in-package :cl-user)

(defpackage :database-core
  (:use :cl :cl-postgres :configuration :cl-ppcre :vip-utils :anaphors)
  (:export #:*database-connection*
           #:*database-now*
           #:database-connection-wrapper
           #:do-with-database
           #:with-database
           #:with-pretend-database
           #:ensure-database
           #:sql-name
           #:symbol-to-sql-name
           #:sql-literal-method #:sql-literal

           #:db-assert-statement
           
           #:debug-connection
           
           #:dquery
           #:dquery/1
           #:db-assert
           #:db-create
           #:db-delete

           #:pg-exec
           #:pg-result
           #:pg-columns
           #:pg-tables
           
           #:with-pg-transaction

           #:solve
           #:sql-query
           #:now
           #:false
           

           #:sql-loop
           #:sql-loop*
           #:sql-get
           #:even/odd
           #:all-columns

           #:do-nested-transaction
           #:do-database-transaction
           #:database-transaction
           #:fail-database-transaction
           ))
