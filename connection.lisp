
(in-package :database-core)

;; provides some wrappers around database connection which uses the configuration system
;; AND allows the connection to be wrapped in something which can do logging, inference etc...

(defparameter *database-connection* nil)

;; This just creates the default database connection...
(defmacro with-database (&body forms)
  `(do-with-database (new-database-connection)
     (lambda () ,@forms)))

(defmacro ensure-database (&body forms)
  `(flet ((f () ,@forms))
     (if *database-connection*
         (f)
         (with-database (f)))))

;; You can always use this if you want a different database connection from the normal one
;; the thing is: most of the time all the code in a certain codebase will connect to the same
;; database.
;; What if we wanted to use a different database for writing as opposed to reading?
;; !!! I have made this a method too so that we can log connection/disconnection
;; that will be useful if we want to know what connection is stuck
(defmethod do-with-database (connection thunk)
  (if connection
      ;; Bind this dynamic var for the sake of things which don't take the conn parameter
      (let ((*database-connection* connection))
        (unwind-protect
             (funcall thunk)
          (pg-disconnect connection)))
      (error "Failed to connect to database")))


(defun new-database-connection ()
  (make-instance (configuration-parameter :database/connection-class
                                          :type 'class
                                          :default (find-class 'database-connection-wrapper))))


(defclass database-connection-wrapper ()
  ((connection :initform 
               (open-database (configuration-parameter :database/name)
                              (configuration-parameter :database/user)
                              (configuration-parameter :database/password :default "")
                              (configuration-parameter :database/host :default "localhost")
                              (configuration-parameter :database/port :type 'integer
                                                       :default 5432))
               :reader connection)))


(def-row-reader core-row-reader (fields)
  (list 
   (cons :tuples
         (loop while (next-row)
            collect (loop for field across fields
                       collect (let ((value (next-field field))) 
                                 ;; Postmodern returns nil values as :null.
                                 ;; pg would return nil, so Abel assumes this all over.
                                 (unless (eq value :null)
                                   value)))))
   (cons :attributes
         (loop for field across fields
            collect (list
                     (field-name field)
                     (field-type field))))))


(defun pg-result (results key)
  (cdr (assoc key results)))


(defmethod pg-columns ((con database-connection-wrapper) table)
  (mapcar #'car (exec-query (connection con) 
                            (format nil "select column_name from information_schema.columns where table_name=~A order by ordinal_position" (sql-literal table))
                            'list-row-reader)))


(defmethod pg-tables ((con database-connection-wrapper))
  (mapcar #'car (exec-query (connection con) 
                            "select table_name from information_schema.tables"
                            'list-row-reader)))


(defmethod pg-exec ((con database-connection-wrapper) &rest sql)
  (exec-query (connection con) (apply #'concatenate 'string sql) 'core-row-reader))


(defmethod pg-exec ((con cl-postgres:database-connection) &rest sql)
  (exec-query con (apply #'concatenate 'string sql) 'core-row-reader))


(defmethod pg-exec ((conn (eql nil)) &rest sql)
  (with-database
      (apply #'pg-exec (cons *database-connection* sql))))


(defmethod pg-disconnect ((x database-connection-wrapper) &key abort)
  (close-database (slot-value x 'connection)))

;; this could probably be used to implement connection pooling and such
;; I think I might need to implement macros to extend/modify the configuration parameters

(defmethod pg-disconnect ((x cl-postgres:database-connection) &key abort)
  (close-database x))
