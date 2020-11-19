
(asdf:defsystem #:database-core
  :description "Core layerable database access"
  :author "VIP"
  :license "vip"
  :depends-on ("cl-postgres" "configuration" "cl-ppcre" "anaphors")
  :serial t
  :components ((:file "package")
               (:file "connection")
               (:file "basic-statements")
               (:file "debug-connection") ; demo of connection layer subclassing
               (:file "sql-loop")
               (:file "transactions")
               (:file "pretend-database")
               ))
