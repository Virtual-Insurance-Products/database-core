
(in-package :database-core)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; A nicer notation for querying the database. The first three
;; functions translate to SQL syntax from this notation.

;; turn a lisp symbol, string or list whose first item is a symbol, into a valid sql identifier following our conventions
;; !!! This probably should be a method
(defmethod sql-name ((x symbol) &optional y)
  (awhen (regex-replace-all "[\\.-]" (string-downcase (symbol-name x)) "_")
    (write-to-string (if y
                         (concatenate 'string it y)
                         it))))

(defmethod sql-name ((x string) &optional y)
  (write-to-string (if y
                       (concatenate 'string x y)
                       x)))

(defmethod sql-name ((x cons) &optional y)
  (declare (ignore y)) ; !!! Surely shouldn't be
  (error "Illegal SQL column name: ~A" x)
  (sql-name (first x)))


;; (sql-name 'my-table)
;; (sql-name 'db.time)
;; (sql-name "my-Table")
;; (sql-name '(my-table foo)) ; why???


(defun symbol-to-sql-name (x)
  (regex-replace "^db_" (regex-replace-all "-" (string-downcase (symbol-name x)) "_")
                 ""))

;; (symbol-to-sql-name 'some-table)
;; (symbol-to-sql-name 'db-underwriter-price)




(defparameter *database-now* "'NOW'")

(defmethod sql-literal-method ((object t))
  (error (s "Invalid SQL value ~S" object)))

;; Use a type to explicitly case things...
;; *** FIXME (or don't use)
(defun sql-literal (value &optional type)
  (declare (ignore type))
  (cond ((stringp value)
         ;; safest to use dollar quoting...
         ;; *** we have to keep trying different tags until we find one not found in the string...
         (if (scan "\\$" value)
             (flet ((marker ()
                      ;; !!! This is a bit nonsensical
                      (s "a~A" (random-string 8))))
               (let ((marker (marker)))
                 (loop while (scan (s "\\$~A\\$" marker) value)
                       do (setf marker (marker)))
                 (s "$~A$~A$~A$" marker value marker)))
             (s "$$~A$$" value)))
        ((integerp value)
         (s "$$~A$$" value))
        ((numberp value)
         ;; *** Find out how to output floating point numbers properly
         (s "$$~a$$" (* 1.0 value))
         
         )
        ((eq value t) "TRUE")
        ((eq value nil) "FALSE")
        ((eq value 'false) "FALSE")
        ((eq value 'now) *database-now*) ; timestamp
        ((eq value 'null) "NULL")
        ((eq value :null) "NULL")
        (t (sql-literal-method value))))



(defparameter *statement-summary* nil)


(defun add-statement (query connection time)
  "query count total-time connection"
  (let ((statement (find query *statement-summary* :test #'equalp :key #'car)))
    (if statement
        (progn
          (incf (cadr statement))
          (setf (caddr statement) (+ (caddr statement) time)))
        (push `(,query
                1 
                ,time 
                ,(format nil "~A" connection))
              *statement-summary*))))

;; a shorthand notation
(defun dquery (q &key include-attributes)
  (let ((result (pg-exec *database-connection* q)))
    (if include-attributes
        (let ((attributes (pg-result result :attributes)))
          (loop for row in (pg-result result :tuples)
                collect (loop for x in row
                              for (y) in attributes
                              collect (cons y x))))
        (pg-result result
                   :tuples))))

(defun dquery/1 (q)
  (first (first (dquery q))))


(defmethod db-assert-statement ((connection t) x &key returning)
  (let ((returning (mapcar #'sql-name
                           (if (listp returning)
                               returning (list returning)))))
    (if (cdr x)
        (s "INSERT INTO ~A (~A) VALUES (~A) ~A;"
           ;; !!! The logic for holding sage actions has been moved out of here
           ;; It would have to be implemented in a combined method on a different connection type
           (sql-name (first x))
           (string-list (loop for (column) on (cdr x) by #'cddr
                           collect (sql-name column))
                        ", ")
           (string-list (loop for (value) on (cddr x) by #'cddr
                           collect (sql-literal value))
                        ", ")
           (if returning
               (s "RETURNING ~{~A~^, ~}" returning)
               ""))
        (s "INSERT INTO ~A DEFAULT VALUES ~A;"
           (sql-name (first x))
           (if returning
               (s "RETURNING ~{~A~^, ~}" returning)
               "")))))


(defun db-assert (x)
  "Insert something into the database. Specify (table-name [:column value]*)"
  ;; !!! I have moved the special handling of string-triple and uri-triple out of here
  ;; this means that would have to be implemented in a connection subclass
  ;; in fact, all it did was avoid duplicate inserts, which wouldn't even always have worked
  ;; I think that can probably be done in the database directly.
  (awhen (db-assert-statement *database-connection* x)
    (dquery it)))



;; this wraps db-assert and returns the id (by convention)
(defun db-create (x &key (returning "id"))
  (awhen (db-assert-statement *database-connection* x :returning returning)
    (if (listp returning)
        (first (dquery it))
        (first (first (dquery it))))))


;; specify part of the table as the first arg, then a list of end bits
;; !!! Probably never used
(defun db-assert-multiple (constants tuples)
  (dolist (a tuples)
    (db-assert (append constants a))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Deletion from database

;; this will fail if we violate constraints (as it should)

;; *** Use with caution - this is not something which will be used very often
(defun db-delete-statement (x)
  (s "DELETE FROM ~A WHERE ~A"
     (sql-name (first x))
     (string-list (loop for (key value) on (cdr x) by #'cddr 
                     collect (s "~A=~A"
                                (sql-name key)
                                (sql-literal value)))
                  " AND ")))

;; (db-delete '(product-requirement :underwriter-product 1 :is-current nil))

(defun db-delete (x)
  "specify a table and constraints of the form (table :key value...) and all rows matching the constraints will be deleted. This is a bit limited - I need to make value specification more general..."
  (dquery (db-delete-statement x)))



(in-package :cl-postgres)


(set-sql-datetime-readers
 :date (lambda (days-since-2000)
         (+ +start-of-2000+
            #+solaris(* 60 (ccl::get-timezone nil))
            (* days-since-2000 +seconds-in-day+)))
 :timestamp (lambda (useconds-since-2000)
              (+ +start-of-2000+
                 #+solaris(* 60 (ccl::get-timezone nil))
                 (floor useconds-since-2000 1000000)))
 :interval (lambda (months days useconds)
             (multiple-value-bind (sec us) (floor useconds 1000000)
               `((:months ,months) (:days ,days) (:seconds ,sec) (:useconds ,us)))))
